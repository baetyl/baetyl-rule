package rule

import (
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/baetyl/baetyl-go/v2/errors"
	"github.com/baetyl/baetyl-go/v2/http"
	"github.com/baetyl/baetyl-go/v2/log"
	routing "github.com/qiangxue/fasthttp-routing"
)

type HandlerFunc func(ctx *routing.Context) (interface{}, error)

func Wrapper(handler HandlerFunc) func(ctx *routing.Context) error {
	return func(ctx *routing.Context) error {
		defer func() {
			if r := recover(); r != nil {
				err, ok := r.(error)
				if !ok {
					err = errors.Trace(fmt.Errorf("unknown error: %w", err))
				}
				log.L().Info("handle a panic", log.Code(err), log.Error(err), log.Any("panic", string(debug.Stack())))
				http.RespondMsg(ctx, 500, "UnknownError", err.Error())
			}
		}()
		res, err := handler(ctx)
		if err != nil {
			log.L().Error("failed to handler request", log.Code(err), log.Error(err))
			return nil
		}
		log.L().Debug("process success", log.Any("response", toJSON(res)))
		http.Respond(ctx, 200, toJSON(res))
		return nil
	}
}

func toJSON(obj interface{}) []byte {
	data, _ := json.Marshal(obj)
	return data
}
