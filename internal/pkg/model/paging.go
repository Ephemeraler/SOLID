package model

import (
    "github.com/go-playground/validator/v10"
)

// PagingQuery represents common pagination parameters.
// Bind from query parameters using Gin: page, page_size.
type PagingQuery struct {
    Page     int `form:"page" json:"page" validate:"omitempty,gte=1"`
    PageSize int `form:"page_size" json:"page_size" validate:"omitempty,gte=1,lte=1000"`
}

// SetDefaults applies defaults and caps according to max size.
func (p *PagingQuery) SetDefaults(defaultPage, defaultSize, maxSize int) {
    if p.Page <= 0 {
        p.Page = defaultPage
    }
    if p.PageSize <= 0 {
        p.PageSize = defaultSize
    }
    if maxSize > 0 && p.PageSize > maxSize {
        p.PageSize = maxSize
    }
}

// Offset returns the SQL offset for the current page.
func (p PagingQuery) Offset() int {
    if p.Page <= 1 {
        return 0
    }
    return (p.Page - 1) * p.PageSize
}

// Limit returns the SQL limit for the current page size.
func (p PagingQuery) Limit() int { return p.PageSize }

// Validate validates the paging parameters using go-playground/validator.
func (p PagingQuery) Validate() error {
    v := validator.New(validator.WithRequiredStructEnabled())
    return v.Struct(p)
}

