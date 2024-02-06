package pipeline

type Data struct {
	Value string
}

//go:generate mockgen -source=pipeline.go -destination=mocks.go -package=pipeline

type Reader interface {
	Read() []*Data
}

type Processor interface {
	Process(d *Data) ([]*Data, error)
}

type Writer interface {
	Write(d []*Data)
}

type Manager interface {
	Manage()
}

type ManagerPipeline struct {
	chunkSize  uint
	reader     Reader
	writer     Writer
	processors []Processor
}

func NewManagerPipeline(
	chunkSize uint,
	reader Reader,
	writer Writer,
	processors []Processor,
) *ManagerPipeline {
	return &ManagerPipeline{
		chunkSize:  chunkSize,
		reader:     reader,
		writer:     writer,
		processors: processors,
	}
}

var _ Manager = (*ManagerPipeline)(nil)

func (m *ManagerPipeline) Manage() {
	m.write(
		m.process(
			m.collect(
				m.read(m.reader),
			),
		),
	)
}

func (m *ManagerPipeline) read(r Reader) <-chan *Data {
	ch := make(chan *Data)

	go func() {
		defer close(ch)

		for _, d := range r.Read() {
			ch <- d
		}
	}()

	return ch
}

func (m *ManagerPipeline) collect(in <-chan *Data) <-chan []*Data {
	ch := make(chan []*Data)

	go func() {
		defer close(ch)

		buf := make([]*Data, 0, m.chunkSize)

		for d := range in {
			buf = append(buf, d)

			if len(buf) == cap(buf) {
				ch <- buf
				buf = make([]*Data, 0, m.chunkSize)
			}
		}
		ch <- buf
	}()

	return ch
}

type processedChunk struct {
	data []*Data
	err  error
}

func (m *ManagerPipeline) process(in <-chan []*Data) <-chan processedChunk {
	ch := make(chan processedChunk)

	go func() {
		defer close(ch)

		for chunk := range in {
			res := processedChunk{
				data: make([]*Data, 0, len(chunk)*len(m.processors)),
			}

			for _, d := range chunk {
				for _, p := range m.processors {
					got, err := p.Process(d)
					if err != nil {
						res.err = err
						break
					}
					res.data = append(res.data, got...)
				}
			}

			ch <- res
		}
	}()

	return ch
}

func (m *ManagerPipeline) write(in <-chan processedChunk) {
	for d := range in {
		if d.err != nil {
			continue
		}
		m.writer.Write(d.data)
	}
}
