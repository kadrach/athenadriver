// Copyright (c) 2022 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package athenadriver

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
)

func TestSQLConnector(t *testing.T) {
	testConf := NewNoOpsConfig()
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}

	conn, err := connector.Connect(context.Background())
	assert.Nil(t, err)
	prepStatement, err := conn.Prepare("select 123")
	assert.Nil(t, err)
	assert.NotNil(t, prepStatement)
	assert.Nil(t, conn.Close())
	transaction, err := conn.Begin()
	assert.Nil(t, transaction)
	assert.Equal(t, err.Error(), "Athena doesn't support transaction statements")
}

func TestSQLConnector_Connect(t *testing.T) {
	testConf := NewNoOpsConfig()
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, LoggerKey, logger)
	ctx = context.WithValue(ctx, MetricsKey, tally.NoopScope)
	conn, err := connector.Connect(ctx)
	assert.Nil(t, err)
	prepStatement, err := conn.Prepare("select 123")
	assert.Nil(t, err)
	assert.NotNil(t, prepStatement)
	assert.Nil(t, conn.Close())
	transaction, err := conn.Begin()
	assert.Nil(t, transaction)
	assert.Equal(t, err.Error(), "Athena doesn't support transaction statements")
}

func TestSQLConnector_Connect_NewSessionFail(t *testing.T) {
	testConf := NewNoOpsConfig()
	_ = testConf.SetRegion("ap-southeast-1")
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
	os.Setenv("AWS_STS_REGIONAL_ENDPOINTS", "123")
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}
	conn, err := connector.Connect(context.Background())
	tx, err := conn.Begin()

	os.Unsetenv("AWS_SDK_LOAD_CONFIG")
	os.Unsetenv("AWS_STS_REGIONAL_ENDPOINTS")
	assert.NotNil(t, err)
	assert.Nil(t, tx)
}

func TestSQLConnector_Connect_NewSession_AWS_SDK_LOAD_CONFIG_true(t *testing.T) {
	testConf := NewNoOpsConfig()
	_ = testConf.SetRegion("ap-southeast-1")
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}
	conn, err := connector.Connect(context.Background())

	os.Unsetenv("AWS_SDK_LOAD_CONFIG")
	assert.Nil(t, err)
	assert.NotNil(t, conn)
}

func TestSQLConnector_Connect_NewSession_AWS_SDK_LOAD_CONFIG_true_AWSProfile_Set(t *testing.T) {
	testConf := NewNoOpsConfig()
	_ = testConf.SetRegion("ap-southeast-1")
	testConf.SetAWSProfile("hello-profile")
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}
	conn, err := connector.Connect(context.Background())

	os.Unsetenv("AWS_SDK_LOAD_CONFIG")
	// In aws-sdk-go-v2 you cannot load a nonexistent profile
	assert.NotNil(t, err)
	assert.Nil(t, conn)
}

func TestSQLConnector_Connect_NewSession_AWS_SDK_LOAD_CONFIG_false(t *testing.T) {
	testConf := NewNoOpsConfig()
	_ = testConf.SetRegion("ap-southeast-1")
	os.Setenv("AWS_SDK_LOAD_CONFIG", "0")
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}
	conn, err := connector.Connect(context.Background())

	os.Unsetenv("AWS_SDK_LOAD_CONFIG")
	assert.Nil(t, err)
	assert.NotNil(t, conn)
}

func TestSQLConnector_Connect_NewSession_Credentials(t *testing.T) {
	testConf := NewNoOpsConfig()
	_ = testConf.SetRegion("ap-southeast-1")
	_ = testConf.SetAccessID("testid")
	_ = testConf.SetSecretAccessKey("testkey")
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}

	conn, err := connector.Connect(context.Background())

	assert.Nil(t, err)
	assert.NotNil(t, conn)
}

func TestSQLConnector_Driver(t *testing.T) {
	testConf := NewNoOpsConfig()
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}
	assert.NotNil(t, connector.Driver())
}

func TestSQLConnector_Connect_UserAgent(t *testing.T) {
	testConf := NewNoOpsConfig()
	connector := &SQLConnector{
		config: testConf,
		tracer: NewDefaultObservability(testConf),
	}

	conn, err := connector.Connect(context.Background())
	assert.Nil(t, err)
	client := conn.(*Connection).athenaClient.(*athena.Client)

	// Capture the built request's User-Agent header and abort before it is sent.
	var userAgent string
	capture := middleware.FinalizeMiddlewareFunc("CaptureUserAgent",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
			middleware.FinalizeOutput, middleware.Metadata, error) {
			userAgent = in.Request.(*smithyhttp.Request).Header.Get("User-Agent")
			return middleware.FinalizeOutput{}, middleware.Metadata{}, errors.New("stop")
		})
	_, err = client.GetQueryExecution(context.Background(),
		&athena.GetQueryExecutionInput{QueryExecutionId: aws.String("qid")},
		func(o *athena.Options) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Finalize.Add(capture, middleware.Before)
			})
		})
	assert.NotNil(t, err)
	assert.Contains(t, userAgent, "aws-sdk-go-v2/")
	assert.Contains(t, userAgent, " athenadriver/"+DriverVersion)
}
