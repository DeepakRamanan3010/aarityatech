package feedserver

import (
	"encoding/json"
	"errors"
)

type feedUpdateRequest struct {
	Mode        int   `json:"m"`
	Instruments []int `json:"i"`
}

type wsRequest struct {
	FeedUpdates []feedUpdateRequest `json:"f"`
}

func (req *wsRequest) unmarshal(jsonBytes []byte) error {
	if err := json.Unmarshal(jsonBytes, &req); err != nil {
		return errors.New("BAD_REQUEST")
	}

	for _, update := range req.FeedUpdates {
		if update.Mode < modeUnsub || update.Mode > modeMarketDepth5 {
			return errors.New("BAD_MODE")
		}
		if len(update.Instruments) == 0 {
			return errors.New("NO_INSTRUMENTS")
		}
	}
	return nil
}
