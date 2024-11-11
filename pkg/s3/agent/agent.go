/*
 Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

// Package agent provides s3 agent and its apis
package agent

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/huawei/cosi-driver/pkg/utils"
)

const (
	defaultRegion = "us-east-1"
	httpTimeOut   = 200 * time.Second
	maxRetries    = 5
)

// S3Agent provides s3 related api
type S3Agent struct {
	Client *s3.S3
}

// Config contains the cfg information required for init S3Agent
type Config struct {
	AccessKey string
	SecretKey string
	Endpoint  string
	RootCA    []byte
}

// NewS3Agent returns a new s3 agent
func NewS3Agent(cfg Config) (*S3Agent, error) {
	// Validate config fields
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	tlsConfig, err := utils.BuildTLSConfig(cfg.RootCA)
	if err != nil {
		return nil, fmt.Errorf("build tls config failed, error is [%v]", err)
	}

	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := http.Client{
		Timeout:   httpTimeOut,
		Transport: tr,
	}

	s, err := session.NewSession(
		aws.NewConfig().
			WithRegion(defaultRegion).
			WithCredentials(credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, "")).
			WithEndpoint(cfg.Endpoint).
			WithS3ForcePathStyle(true).
			WithMaxRetries(maxRetries).
			WithHTTPClient(&client),
	)
	if err != nil {
		return nil, err
	}

	return &S3Agent{
		Client: s3.New(s),
	}, nil
}

// validateConfig validates required fields in the Config struct
func validateConfig(cfg Config) error {
	if cfg.Endpoint == "" {
		return fmt.Errorf("endpoint is empty")
	}

	if _, err := url.Parse(cfg.Endpoint); err != nil {
		return fmt.Errorf("url parse endpoint [%s] failed, error is [%v]", cfg.Endpoint, err)
	}

	if cfg.AccessKey == "" {
		return fmt.Errorf("access key is empty")
	}

	if cfg.SecretKey == "" {
		return fmt.Errorf("secret key is empty")
	}

	return nil
}
