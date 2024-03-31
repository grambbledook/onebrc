package beauty

import (
	"fmt"
	"strings"
)

const (
	width      = 50
	completion = "█"
	incomplete = "░"
)

type state struct {
	done     int
	todo     int
	percents float32
}

type ProgressBar struct {
	total    int
	progress int
	width    int
	chanel   chan state
}

func NewProgressBar(total int) *ProgressBar {
	progressBar := &ProgressBar{
		total:    total,
		progress: 0,
		width:    width,
		chanel:   make(chan state),
	}

	go draw(progressBar.chanel)
	return progressBar
}

func (p *ProgressBar) Increment() {
	p.progress++

	if p.progress > p.total {
		return
	}

	done := p.progress * p.width / p.total
	todo := p.width - done

	percents := float32(p.progress) * 100 / float32(p.total)

	p.chanel <- state{done, todo, percents}

	if p.progress == p.total {
		close(p.chanel)
	}
}

func draw(chanel chan state) {
	previousReading := -1

	for p := range chanel {
		percents := int(p.percents)

		if percents%2 != 0 || previousReading == percents {
			continue
		}

		done := strings.Repeat(completion, p.done)
		todo := strings.Repeat(incomplete, p.todo)

		previousReading = percents
		fmt.Printf("\r%s %d%%", "["+done+todo+"]", percents)
	}
	fmt.Println()
}
