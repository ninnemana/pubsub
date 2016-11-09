package psmongo

import (
	"context"
	"fmt"
	"testing"

	ctx "golang.org/x/net/context"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type pubsubAdmin struct {
	ctx     context.Context
	project string
	opt     option.ClientOption
}

// func NewClient(ctx context.Context, proj string, opt ...option.ClientOption) (*pubsub.Client, error) {
// 	return nil, fmt.Errorf("not implemented")
// }

func TestNewBackgroundClientNoProject(t *testing.T) {
	oldPsClient := psClient
	defer func() {
		psClient = oldPsClient
	}()
	psClient = func(c ctx.Context, proj string, opt ...option.ClientOption) (*pubsub.Client, error) {
		t.Log(c)
		return nil, fmt.Errorf("not implemented")
	}

	_, err := NewBackgroundClient("")
	if err == nil {
		t.Fatalf("error should not be nil: %+v", err)
	}
}

func TestNewBackgroundClient(t *testing.T) {
	client, err := NewBackgroundClient("pusub-testing")
	if err != nil {
		t.Fatalf("failed to connect with real project")
	}

	_, err = client.ListTopics()
	if err != nil {
		t.Fatalf("client doesn't have a stable connection %+v", err)
	}
}

func TestNewClient(t *testing.T) {
	_, err := NewClient("", "", "", nil)
	if err == nil {
		t.Fatalf("error should not be nil: %+v", err)
	}
}
