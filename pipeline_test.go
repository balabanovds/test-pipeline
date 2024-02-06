package pipeline

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const chunkSize = 3

func TestStageRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	r := NewMockReader(ctrl)
	m := NewManagerPipeline(chunkSize, nil, nil, nil)

	data := []*Data{
		testData(t, "uno"),
		testData(t, "duo"),
		testData(t, "tres"),
		testData(t, "quatro"),
	}

	r.EXPECT().Read().Return(data)

	var idx int
	for d := range m.read(r) {
		require.Equal(t, data[idx], d)
		idx++
	}
}

func TestStageCollect(t *testing.T) {
	data := []*Data{
		testData(t, "uno"),
		testData(t, "duo"),
		testData(t, "tres"),
		testData(t, "quatro"),
	}

	wantChunk1 := []*Data{
		testData(t, "uno"),
		testData(t, "duo"),
		testData(t, "tres"),
	}

	wantChunk2 := []*Data{
		testData(t, "quatro"),
	}

	m := NewManagerPipeline(chunkSize, nil, nil, nil)
	inch := make(chan *Data)
	gotch := m.collect(inch)

	go func() {
		defer close(inch)
		for _, d := range data {
			d := d
			t.Logf("sending to incoming chanel data %v", d)
			inch <- d
		}
	}()

	t.Log("waiting for chunk 1")
	got1 := <-gotch
	require.Equal(t, wantChunk1, got1)

	t.Log("waiting for chunk 2")
	got2 := <-gotch
	require.Equal(t, wantChunk2, got2)

	t.Log("waiting for chanel closed")
	_, ok := <-gotch
	require.False(t, ok)
}

func TestStageProcess(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockProcessor(ctrl)
	p2 := NewMockProcessor(ctrl)
	m := NewManagerPipeline(chunkSize, nil, nil, []Processor{p1, p2})

	data := testData(t, "uno")

	p1OutData := []*Data{
		testData(t, "uno+p1"),
		testData(t, "duo+p1"),
	}
	p2OutData := []*Data{
		testData(t, "uno+p2"),
		testData(t, "duo+p2"),
	}

	processChunkData := []*Data{}
	processChunkData = append(processChunkData, p1OutData...)
	processChunkData = append(processChunkData, p2OutData...)

	wantChunk := processedChunk{
		data: processChunkData,
		err:  nil,
	}

	inch := make(chan []*Data)
	gotch := m.process(inch)

	p1.EXPECT().Process(data).Return(p1OutData, nil)
	p2.EXPECT().Process(data).Return(p2OutData, nil)

	go func() {
		defer close(inch)
		inch <- []*Data{data}
	}()

	t.Log("waiting to get processed chunk")
	got, ok := <-gotch
	require.Equal(t, wantChunk, got)
	require.True(t, ok)

	t.Log("waiting out chanel to be closed")
	_, ok = <-gotch
	require.False(t, ok)
}

func TestStageProcess_error(t *testing.T) {
	ctrl := gomock.NewController(t)
	p1 := NewMockProcessor(ctrl)
	p2 := NewMockProcessor(ctrl)
	m := NewManagerPipeline(chunkSize, nil, nil, []Processor{p1, p2})
	p2err := errors.New("")

	data := testData(t, "uno")

	p1OutData := []*Data{
		testData(t, "uno+p1"),
		testData(t, "duo+p1"),
	}

	processChunkData := []*Data{}
	processChunkData = append(processChunkData, p1OutData...)

	wantChunk := processedChunk{
		data: processChunkData,
		err:  p2err,
	}

	inch := make(chan []*Data)
	gotch := m.process(inch)

	p1.EXPECT().Process(data).Return(p1OutData, nil)
	p2.EXPECT().Process(data).Return(nil, p2err)

	go func() {
		defer close(inch)
		inch <- []*Data{data}
	}()

	t.Log("waiting to get processed chunk")
	got, ok := <-gotch
	require.Equal(t, wantChunk, got)
	require.True(t, ok)

	t.Log("waiting out chanel to be closed")
	_, ok = <-gotch
	require.False(t, ok)
}

func TestStageWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	w := NewMockWriter(ctrl)
	m := NewManagerPipeline(chunkSize, nil, w, nil)

	makeDataSlice := func(prefix string) []*Data {
		return []*Data{
			testData(t, prefix+"uno"),
			testData(t, prefix+"duo"),
		}
	}

	noerrData := makeDataSlice("noerr")
	errData := makeDataSlice("err")

	incomingData := []processedChunk{
		{data: noerrData},
		{data: errData, err: errors.New("")},
	}

	w.EXPECT().Write(noerrData)

	inch := make(chan processedChunk)

	go func() {
		defer close(inch)
		for _, d := range incomingData {
			d := d
			inch <- d
		}
	}()

	m.write(inch)
}

func testData(t *testing.T, value string) *Data {
	t.Helper()

	return &Data{Value: value}
}
