package response

import (
	"encoding/json"
	"net/url"
	"strconv"
)

type Response struct {
	Count    int         `json:"count"`
	Previous url.URL     `json:"previous" swaggertype:"string"`
	Next     url.URL     `json:"next" swaggertype:"string"`
	Results  interface{} `json:"results"`
	Detail   string      `json:"detail"`
}

// MarshalJSON renders Previous and Next as URL strings instead of struct fields.
func (r Response) MarshalJSON() ([]byte, error) {
	type alias struct {
		Count    int         `json:"count"`
		Previous string      `json:"previous"`
		Next     string      `json:"next"`
		Results  interface{} `json:"results"`
		Detail   string      `json:"detail"`
	}

	a := alias{
		Count:    r.Count,
		Previous: r.Previous.String(),
		Next:     r.Next.String(),
		Results:  r.Results,
		Detail:   r.Detail,
	}
	return json.Marshal(a)
}

// BuildPageLinks constructs previous and next page URLs based on the provided
// base URL and paging parameters. It does not modify the input URL.
func BuildPageLinks(base *url.URL, page, pageSize, total int) (prev, next url.URL) {
	if base == nil || pageSize <= 0 {
		return url.URL{}, url.URL{}
	}
	lastPage := (total + pageSize - 1) / pageSize

	makeURL := func(p int) url.URL {
		if p < 1 {
			p = 1
		}
		u := *base
		q := u.Query()
		q.Set("page", strconv.Itoa(p))
		q.Set("page_size", strconv.Itoa(pageSize))
		u.RawQuery = q.Encode()
		return u
	}

	if page > 1 {
		prev = makeURL(page - 1)
	}
	if page < lastPage {
		next = makeURL(page + 1)
	}
	return
}
