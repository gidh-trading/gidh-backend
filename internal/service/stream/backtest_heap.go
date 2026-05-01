package stream

import (
	"gidh-backend/internal/service/models"
)

// heapItem holds one element in the k-way merge.
type heapItem struct {
	tick     models.TickData
	iterator *tickIterator // Tracks the source stream
}

// tickHeap is a min-heap of *heapItem, ordered by tick.Timestamp
type tickHeap []*heapItem

func (h *tickHeap) Len() int           { return len(*h) }
func (h *tickHeap) Less(i, j int) bool { return (*h)[i].tick.Timestamp.Before((*h)[j].tick.Timestamp) }
func (h *tickHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *tickHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *tickHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
