package client

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	gcontext "github.com/baetyl/baetyl-go/v2/context"
	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	"github.com/baetyl/baetyl-go/v2/mqtt"
	v1 "github.com/baetyl/baetyl-go/v2/spec/v1"
	"github.com/baetyl/baetyl-go/v2/utils"

	"github.com/baetyl/baetyl-rule/v2/config"
)

// EventType the type of event from cloud
type EventType string

// Event event message
type Event struct {
	Time    time.Time   `json:"time"`
	Type    EventType   `json:"type"`
	Content UploadEvent `json:"content"`
}

// UploadEvent update event
type UploadEvent struct {
	RemotePath string `yaml:"remotePath" json:"remotePath" validate:"nonzero"`
	LocalPath  string `yaml:"localPath" json:"localPath" validate:"nonzero"`
}

type S3ClientCfg struct {
	Address string `yaml:"address" json:"address"`
	Region  string `yaml:"region" json:"region" default:"us-east-1"`
	Ak      string `yaml:"ak" json:"ak"`
	Sk      string `yaml:"sk" json:"sk"`
	Bucket  string `yaml:"bucket" json:"bucket"`
	Token   string `yaml:"token,omitempty" json:"token,omitempty" default:""`
}

type S3Client struct {
	cfg          *S3ClientCfg
	cli          *http.Client
	s3Client     *s3.S3
	uploader     *s3manager.Uploader
	tasks        chan *UploadEvent
	stsDeadline  time.Time
	remotePrefix string
	tomb         utils.Tomb
	logger       *log.Logger
}

func NewS3Client(ctx gcontext.Context, cfg *S3ClientCfg) (Client, error) {
	client := &S3Client{
		s3Client:    &s3.S3{},
		cfg:         cfg,
		tasks:       make(chan *UploadEvent, config.TaskLength),
		uploader:    &s3manager.Uploader{},
		stsDeadline: time.Now(),
		logger:      log.With(log.Any("storage", "s3")),
	}
	if cfg.Ak == "" && cfg.Sk == "" {
		cli, err := ctx.NewCoreHttpClient()
		if err != nil {
			return nil, errors.Trace(err)
		}
		client.cli = cli
		return client, nil
	}
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(cfg.Ak, cfg.Sk, cfg.Token),
		Endpoint:         aws.String(cfg.Address),
		Region:           aws.String(cfg.Region),
		DisableSSL:       aws.Bool(!strings.HasPrefix(cfg.Address, "https")),
		S3ForcePathStyle: aws.Bool(true),
	}
	sessionProvider, err := session.NewSession(s3Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	client.s3Client = s3.New(sessionProvider)
	client.uploader = s3manager.NewUploader(sessionProvider)
	return client, nil
}

func (s *S3Client) FileExists(Bucket, remotePath string) bool {
	cparams := &s3.HeadObjectInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(remotePath),
	}
	_, err := s.s3Client.HeadObject(cparams)
	if err != nil {
		return false
	}
	return true
}

func (s *S3Client) Upload(f, remotePath string) error {
	res, err := s.RefreshSts()
	if err != nil {
		return errors.Trace(err)
	}
	if res != nil {
		s.remotePrefix = res.Namespace + "/" + res.NodeName
		s.stsDeadline = res.Expiration
		s.cfg.Bucket = res.Bucket
		s.cfg.Ak = res.AK
		s.cfg.Sk = res.SK
		s.cfg.Address = res.Endpoint
		s.cfg.Token = res.Token
	}
	// rewrite remote path with default path
	if s.cli != nil {
		remotePath = s.remotePrefix + "/" + remotePath
	}
	if s.FileExists(s.cfg.Bucket, remotePath) {
		s.logger.Warn("file exist", log.Any("remote", remotePath))
		return nil
	}
	return s.PutObjectFromFile(s.cfg.Bucket, remotePath, f)
}

func (s *S3Client) PutObjectFromFile(Bucket, remotePath, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()
	params := &s3manager.UploadInput{
		Bucket: aws.String(Bucket),     // Required
		Key:    aws.String(remotePath), // Required
		Body:   f,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err = s.uploader.UploadWithContext(ctx, params, func(u *s3manager.Uploader) {
		u.LeavePartsOnError = true
	})
	return err
}

func (s *S3Client) RefreshSts() (*v1.STSResponse, error) {
	if s.cli == nil {
		return nil, nil
	}
	if s.stsDeadline.UTC().After(time.Now().UTC()) {
		return nil, nil
	}
	res, err := GetSts(s.cli)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.logger.Debug("Refresh minio sts")
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(res.AK, res.SK, res.Token),
		Endpoint:         aws.String(res.Endpoint),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(!strings.HasPrefix(res.Endpoint, "https")),
		S3ForcePathStyle: aws.Bool(true),
	}
	sessionProvider, err := session.NewSession(s3Config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.s3Client = s3.New(sessionProvider)
	s.uploader = s3manager.NewUploader(sessionProvider)
	return res, nil
}

func GetSts(cli *http.Client) (*v1.STSResponse, error) {
	var err error
	req := &v1.STSRequest{
		STSType: "minio",
	}
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	body, err := cli.PostJSON("/agent/sts", reqBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var stsInfo v1.STSResponse
	if err = json.Unmarshal(body, &stsInfo); err != nil {
		return nil, errors.Trace(err)
	}
	return &stsInfo, nil
}

func (s *S3Client) SendOrDrop(pkt *config.TargetMsg) error {
	var e Event
	err := json.Unmarshal(pkt.Data, &e)
	if err != nil {
		return errors.New("Unexpected message content")
	}
	select {
	case <-s.tomb.Dying():
		return errors.New("ctx done")
	case s.tasks <- &e.Content:
		return nil
	}
}

func (s *S3Client) SendPubAck(_ mqtt.Packet) error {
	return nil
}

func (s *S3Client) Start(_ mqtt.Observer) error {
	return s.tomb.Go(func() error {
		for {
			select {
			case <-s.tomb.Dying():
				return nil
			case task := <-s.tasks:
				err := s.Upload(task.LocalPath, task.RemotePath)
				if err != nil {
					s.logger.Error("failed to Upload file", log.Error(err))
				}
			}
		}
	})
}

func (s *S3Client) ResetClient(_ *mqtt.ClientConfig) {}

func (s *S3Client) SetReconnectCallback(_ mqtt.ReconnectCallback) {}

// Close closes client
func (s *S3Client) Close() error {
	s.tomb.Kill(nil)
	err := s.tomb.Wait()
	if err != nil {
		s.logger.Error("failed to wait on tomb", log.Error(err))
	}
	return nil
}
