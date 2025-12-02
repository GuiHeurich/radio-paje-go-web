package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/joho/godotenv"
)

type B2Client struct {
	bucketName string
	s3Client   *s3.Client
}

type B2 interface {
	listFiles() ([]string, error)
	selectRandomFile(fileNames []string) (string, error)
	downloadFile(fileName string) (string, error)
}

func NewB2Client(endpoint, region, keyId, applicationKey, bucketName string) (B2, error) {
	ctx := context.Background()

	// Create custom credentials provider
	credProvider := credentials.NewStaticCredentialsProvider(keyId, applicationKey, "")

	// Load config with custom endpoint and credentials
	sdkConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credProvider),
	)
	if err != nil {
		log.Printf("Couldn't load configuration: %v", err)
		return nil, err
	}

	// Create S3 client with B2 endpoint
	s3Client := s3.NewFromConfig(sdkConfig, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // B2 requires path-style addressing
	})

	return &B2Client{
		bucketName: bucketName,
		s3Client:   s3Client,
	}, nil
}

func (b *B2Client) listFiles() ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucketName),
	}

	result, err := b.s3Client.ListObjectsV2(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	var fileNames []string
	for _, object := range result.Contents {
		fileNames = append(fileNames, *object.Key)
	}

	return fileNames, nil
}

func (b *B2Client) selectRandomFile(fileNames []string) (string, error) {
	if len(fileNames) == 0 {
		return "", errors.New("no files found")
	}

	randomIndex := rand.Intn(len(fileNames))
	return fileNames[randomIndex], nil
}

func (b *B2Client) downloadFile(fileName string) (string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(fileName),
	}

	log.Printf("Downloading file: %s from bucket: %s", fileName, b.bucketName)

	output, err := b.s3Client.GetObject(context.TODO(), input)
	if err != nil {
		return "", fmt.Errorf("failed to get object: %w", err)
	}
	defer output.Body.Close()

	filePath := fmt.Sprintf("cache/%s", fileName)

	// Create directory structure if needed
	dir := "cache"
	if strings.Contains(fileName, "/") {
		parts := strings.Split(fileName, "/")
		dir = fmt.Sprintf("cache/%s", strings.Join(parts[:len(parts)-1], "/"))
	}

	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to create cache directory: %w", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, output.Body)
	if err != nil {
		return "", fmt.Errorf("failed to copy file content: %w", err)
	}

	log.Printf("Successfully cached file to: %s", filePath)
	return filePath, nil
}

func stream(w http.ResponseWriter, req *http.Request) {
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: Error loading .env file: %v", err)
	}

	keyId := os.Getenv("KEY_ID")
	applicationKey := os.Getenv("APPLICATION_KEY")
	bucketName := os.Getenv("BUCKET_NAME")
	endpoint := os.Getenv("ENDPOINT")
	region := os.Getenv("REGION")

	// Validate required environment variables
	if keyId == "" || applicationKey == "" || bucketName == "" || endpoint == "" {
		http.Error(w, "Missing required environment variables", http.StatusInternalServerError)
		log.Printf("Missing environment variables")
		return
	}

	// Default region if not specified
	if region == "" {
		region = "us-east-5"
	}

	log.Printf("Connecting to B2 - Endpoint: %s, Region: %s, Bucket: %s", endpoint, region, bucketName)

	b2Client, err := NewB2Client(endpoint, region, keyId, applicationKey, bucketName)
	if err != nil {
		http.Error(w, "Failed to create B2 client", http.StatusInternalServerError)
		log.Printf("Failed to create B2 client: %v", err)
		return
	}

	fileName := req.URL.Query().Get("file")

	// If no file specified, select random file and redirect
	if fileName == "" {
		listResult, err := b2Client.listFiles()
		if err != nil {
			http.Error(w, "Failed to list files", http.StatusInternalServerError)
			log.Printf("Failed to list files: %v", err)
			return
		}

		randomFile, err := b2Client.selectRandomFile(listResult)
		if err != nil {
			http.Error(w, "No files available", http.StatusNotFound)
			log.Printf("Failed to select random file: %v", err)
			return
		}

		log.Printf("Selected random file: %s", randomFile)

		// Properly URL encode the filename
		encodedFile := strings.Replace(randomFile, " ", "%20", -1)
		encodedFile = strings.Replace(encodedFile, "#", "%23", -1)
		encodedFile = strings.Replace(encodedFile, "?", "%3F", -1)

		http.Redirect(w, req, fmt.Sprintf("/stream?file=%s", encodedFile), http.StatusFound)
		return
	}

	log.Printf("Fetching file: %s", fileName)

	// Download the file
	filePath, err := b2Client.downloadFile(fileName)
	if err != nil {
		http.Error(w, "Failed to download file", http.StatusInternalServerError)
		log.Printf("Failed to download file: %v", err)
		return
	}

	// Log range header for debugging
	rangeHeader := req.Header.Get("Range")
	if rangeHeader != "" {
		log.Printf("Range header: %s", rangeHeader)
	}

	// Serve the file (supports range requests automatically)
	http.ServeFile(w, req, filePath)
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/stream", stream)

	log.Println("Server starting on :8090")
	if err := http.ListenAndServe(":8090", nil); err != nil {
		log.Fatal(err)
	}
}
