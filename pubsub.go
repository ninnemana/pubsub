package pubsub

import (
	"os"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/cloud"
	ps "google.golang.org/cloud/pubsub"
)

var (
	pubsubCtx context.Context
	// OAuthEmail Is the e-mail address used to authenticating
	// against Google Cloud Platform
	OAuthEmail = os.Getenv("GOOGLE_OAUTH_EMAIL")
	// ClientKey Is the private key generated from Google Cloud Platform
	// that cooresponds to the OAuthEmail.
	ClientKey = os.Getenv("GOOGLE_CLIENT_KEY")
	projectID = "curt-groups"

	pubsubScopes = []string{
		ps.ScopeCloudPlatform,
		ps.ScopePubSub,
	}
)

// NewContext Generates a context.Context that is authenticated against
// Google Cloud Platform with the `scopes` provided.
func NewContext(scopes []string) error {
	if OAuthEmail == "" {
		return backgrounContext(scopes)
	}

	return jwtContext(scopes)
}

func jwtContext(scopes []string) error {
	conf := &jwt.Config{
		Email:      OAuthEmail,
		PrivateKey: []byte(ClientKey),
		Scopes:     scopes,
		TokenURL:   google.JWTTokenURL,
	}

	pubsubCtx = cloud.NewContext(projectID, conf.Client(oauth2.NoContext))

	return pubsubCtx.Err()
}

func backgrounContext(scopes []string) error {
	ctx := context.Background()
	c, err := google.DefaultClient(ctx, scopes...)
	if err != nil {
		return err
	}

	pubsubCtx = cloud.WithContext(ctx, projectID, c)

	return pubsubCtx.Err()
}

// PushMessage Will send the `msgs` to Google PubSub into the `topic` that
// is provided. If the `topic` doesn't exist, it will be created.
func PushMessage(topic string, msgs ...*ps.Message) error {
	var err error
	if pubsubCtx == nil {
		NewContext(pubsubScopes)
	}

	err = createTopic(topic)
	if err != nil {
		return err
	}

	_, err = ps.Publish(pubsubCtx, topic, msgs...)
	return err

}

func createTopic(topic string) error {
	exists, err := ps.TopicExists(pubsubCtx, topic)
	if err != nil || exists {
		return err
	}

	return ps.CreateTopic(pubsubCtx, topic)
}
