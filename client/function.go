package client

import (
	"bytes"
	"io/ioutil"
	gohttp "net/http"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
)

type Function struct {
	URL string
	CLI *http.Client
}

func (f *Function) Post(payload []byte) ([]byte, error) {
	resp, err := f.CLI.PostURL(f.URL, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != gohttp.StatusOK {
		return nil, errors.Errorf("[%d] %s", resp.StatusCode, resp.Status)
	}
	return data, nil
}
