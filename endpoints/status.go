package endpoints

import (
	"net/http"

	"github.com/alice-lg/birdwatcher/bird"
	"github.com/julienschmidt/httprouter"
)

func Status(r *http.Request, ps httprouter.Params, useCache bool) (bird.Parsed, bool) {
	res, fromCache := bird.Status(useCache)

	// Attach socket pool status when the socket backend is active
	if bird.ClientConf.Backend == "socket" {
		if pool := bird.GetGlobalSocketPool(); pool != nil {
			res["socket_pool"] = pool.Status()
		}
	}

	return res, fromCache
}
