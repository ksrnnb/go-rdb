package file

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// FileManager は特定のブロックの内容をページに読み込んだり、ページの内容をブロックに書き込んだりする
// ファイルへのアクセスはブロック単位で行う
type FileManager struct {
	dbDirectory string
	blockSize   int
	isNew       bool
	openFiles   map[string]*os.File
	mux         sync.Mutex
}

// NewFileManager はシステム起動時に SimpleDB によって実行される
func NewFileManager(dirname string, bs int) (*FileManager, error) {
	var isNew bool
	if _, err := os.Stat(dirname); os.IsNotExist(err) {
		isNew = true
	}

	dbDirectory, err := createDirectoryIfNeeded(dirname)
	if err != nil {
		log.Fatalf("NewSimpleDB() failed, %v", err)
	}

	fm := &FileManager{
		dbDirectory: dbDirectory,
		blockSize:   bs,
		isNew:       isNew,
		openFiles:   map[string]*os.File{},
	}

	return fm, nil
}

// 指定したブロック領域をページに読み込む
func (fm *FileManager) Read(blk BlockID, p *Page) error {
	fm.mux.Lock()
	defer fm.mux.Unlock()
	f, err := fm.getFile(blk.FileName())

	if err != nil {
		return fmt.Errorf("file: Read() failed to get file from BlockID, %w", err)
	}

	_, err = f.Seek(int64(blk.Number()*fm.blockSize), 0)
	if err != nil {
		return err
	}

	b := make([]byte, fm.blockSize)
	n, ioErr := f.Read(b)

	// Callers should always process the n > 0 bytes returned before
	// considering the error err.
	// Doing so correctly handles I/O errors that happen after
	// reading some bytes and also both of the allowed EOF behaviors.
	if n > 0 {
		resizedBuf := make([]byte, n)
		copy(resizedBuf, b[:n])

		if err := p.WriteBuf(resizedBuf); err != nil {
			return fmt.Errorf("file: Read() failed to write buffer to page, %w", err)
		}
	}

	if ioErr != nil && !errors.Is(ioErr, io.EOF) {
		return fmt.Errorf("file: Read() failed to read file to buffer, %w", err)
	}

	return nil
}

// 指定したブロック位置にページの内容を全て書き込む
func (fm *FileManager) Write(blk BlockID, p *Page) error {
	fm.mux.Lock()
	defer fm.mux.Unlock()
	f, err := fm.getFile(blk.FileName())

	if err != nil {
		return err
	}

	f.Seek(int64(blk.Number()*fm.blockSize), 0)
	_, err = f.Write(p.ReadBuf())

	return err
}

// Append()は新しく空のブロックを作成して、指定したファイルに割り当てる。
func (fm *FileManager) Append(filename string) (BlockID, error) {
	fm.mux.Lock()
	defer fm.mux.Unlock()
	newBlkNum, err := fm.Length(filename)
	if err != nil {
		return BlockID{}, err
	}

	blk := NewBlockID(filename, newBlkNum)

	f, err := fm.getFile(filename)
	if err != nil {
		return BlockID{}, err
	}

	_, err = f.Seek(int64(blk.Number()*fm.blockSize), 0)
	if err != nil {
		return BlockID{}, err
	}

	b := make([]byte, fm.blockSize)
	_, err = f.Write(b)
	if err != nil {
		return BlockID{}, err
	}

	return blk, nil
}

// Length()は指定したファイルの長さ（=ブロックNo.）を取得する
// 具体的には、指定したファイルのサイズをブロックサイズで割る。
func (fm *FileManager) Length(filename string) (int, error) {
	f, err := fm.getFile(filename)

	if err != nil {
		return 0, err
	}

	fs, err := f.Stat()

	if err != nil {
		return 0, err
	}

	return int(fs.Size()) / fm.blockSize, nil
}

// IsNew()は、DBのディレクトリを新規に作成したかどうかを返す
func (fm *FileManager) IsNew() bool {
	return fm.isNew
}

// BlockSize()はFileManagerがもつブロックサイズを返す
func (fm *FileManager) BlockSize() int {
	return fm.blockSize
}

func (fm *FileManager) getFile(filename string) (*os.File, error) {
	f, ok := fm.openFiles[filename]

	if ok {
		return f, nil
	}

	fp := filepath.Join(fm.dbDirectory, filename)
	f, err := os.OpenFile(fp, os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		return nil, err
	}

	fm.openFiles[filename] = f
	return f, nil
}

// createDirectoryIfNeeded()はディレクトリ名を引数にとり、
// 兄弟となる階層にディレクトリが存在しなければ作成、存在すればパスを返す
func createDirectoryIfNeeded(dirname string) (dbDirectory string, err error) {
	dbDirectory = filepath.Join(ProjectRootDir(), dirname)

	_, err = os.Stat(dbDirectory)
	if os.IsNotExist(err) {
		err = os.Mkdir(dbDirectory, 0744)

		if err != nil {
			return "", fmt.Errorf("newDirectory() failed, %v", err)
		}
	}

	if err != nil {
		return "", err
	}

	return dbDirectory, nil
}

func ProjectRootDir() string {
	_, file, _, _ := runtime.Caller(0)
	currentDir := filepath.Dir(file)
	return filepath.Join(currentDir, "..")
}
