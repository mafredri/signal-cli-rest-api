package api

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"net/url"
	"strings"
)

type PatchProfileRequest struct {
	Name         *string `json:"name,omitempty"`
	Base64Avatar *string `json:"base64_avatar,omitempty"`
	RemoveAvatar bool    `json:"remove_avatar,omitempty"`
}

// PatchProfile changes only supplied profile fields. PUT retains its existing semantics.
func (a *Api) PatchProfile(c *gin.Context) {
	number, err := url.PathUnescape(c.Param("number"))
	if err != nil || number == "" {
		c.JSON(400, Error{Msg: "Registered number is required"})
		return
	}
	var req PatchProfileRequest
	if err = c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, Error{Msg: "Invalid profile update"})
		return
	}
	if (req.Name == nil && req.Base64Avatar == nil && !req.RemoveAvatar) || (req.Name != nil && strings.TrimSpace(*req.Name) == "") || (req.Base64Avatar != nil && (*req.Base64Avatar == "" || req.RemoveAvatar)) {
		c.JSON(400, Error{Msg: "Supply profile fields; avatar replacement and removal are mutually exclusive"})
		return
	}
	if err = a.signalClient.PatchProfile(number, req.Name, req.Base64Avatar, req.RemoveAvatar, nil); err != nil {
		c.JSON(400, Error{Msg: err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}
