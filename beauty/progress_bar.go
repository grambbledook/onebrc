package beauty

import (
	"fmt"
	"strings"
	"sync"
)

const (
	width      = 50
	completion = "█"
	incomplete = "░"
)

type ProgressBar struct {
	total    int
	progress int
	width    int
	mutex    sync.Mutex
}

func NewProgressBar(total int) *ProgressBar {
	return &ProgressBar{
		total:    total,
		progress: 0,
		width:    width,
		mutex:    sync.Mutex{},
	}
}

func (p *ProgressBar) Increment() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.progress += 1
	p.Draw()
}

func (p *ProgressBar) Draw() {
	if p.progress > p.total {
		return
	}

	completed := p.progress * p.width / p.total
	remaining := p.width - completed

	done := strings.Repeat(completion, completed)
	not := strings.Repeat(incomplete, remaining)

	percents := float32(p.progress) * 100 / float32(p.total)
	fmt.Printf("\r%s %d%%", "["+done+not+"]", int(percents))

	if p.progress == p.total {
		fmt.Println()
	}
}
