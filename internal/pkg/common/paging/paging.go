package paging

type PagingQuery struct {
	Paging   bool `form:"paging,default=true" json:"paging"`
	Page     int  `form:"page,default=1" json:"page" binding:"gte=1"`
	PageSize int  `form:"page_size,default=20" json:"page_size" binding:"min=1,max=100"`
}
