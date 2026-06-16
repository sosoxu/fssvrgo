package tests

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/sosoxu/fssvrgo/internal/utils"
	"github.com/sosoxu/fssvrgo/tests/testutil"
)

func TestGRPCUploadFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	data := []byte("hello world")
	meta, err := ts.FM.UploadFile("test.txt", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	if meta.Path != "test.txt" {
		t.Errorf("expected path test.txt, got %s", meta.Path)
	}
	if meta.Name != "test.txt" {
		t.Errorf("expected name test.txt, got %s", meta.Name)
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), meta.Size)
	}
	if meta.Hash != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Errorf("hash mismatch")
	}
	if meta.StorageType != "local" {
		t.Errorf("expected storage type local, got %s", meta.StorageType)
	}
	if meta.IsDeleted {
		t.Errorf("expected IsDeleted false")
	}
	if meta.ID == "" {
		t.Errorf("expected non-empty ID")
	}
	if meta.CreatedAt == "" {
		t.Errorf("expected non-empty CreatedAt")
	}
}

func TestGRPCUploadLargeFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	data := make([]byte, 5*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	meta, err := ts.FM.UploadFile("large.bin", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	if meta.Size != int64(len(data)) {
		t.Errorf("expected size %d, got %d", len(data), meta.Size)
	}
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))
	if meta.Hash != expectedHash {
		t.Errorf("hash mismatch")
	}
}

func TestGRPCDownloadFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	original := []byte("download me")
	_, err = ts.FM.UploadFile("dl.txt", original)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	downloaded, err := ts.FM.DownloadFile("dl.txt")
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	if !bytes.Equal(downloaded, original) {
		t.Errorf("downloaded content does not match original")
	}
}

func TestGRPCDownloadFileAt(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = ts.FM.UploadFile("offset.bin", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	offset := int64(500)
	size := 100
	result, err := ts.FM.DownloadFileAt("offset.bin", size, offset)
	if err != nil {
		t.Fatalf("DownloadFileAt failed: %v", err)
	}

	expected := data[offset : offset+int64(size)]
	if !bytes.Equal(result, expected) {
		t.Errorf("downloaded slice does not match expected range")
	}
}

func TestGRPCListFiles(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	ts.FM.UploadFile("file1.txt", []byte("a"))
	ts.FM.UploadFile("file2.txt", []byte("b"))
	ts.FM.UploadFile("file3.txt", []byte("c"))
	ts.DirSvc.CreateDirectory("subdir")

	result, err := ts.FlSvc.ListFiles("", false, 1, 2, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}

	if result.Total < 4 {
		t.Errorf("expected total >= 4, got %d", result.Total)
	}
	if result.Page != 1 {
		t.Errorf("expected page 1, got %d", result.Page)
	}
	if result.PageSize != 2 {
		t.Errorf("expected page size 2, got %d", result.PageSize)
	}
	if len(result.Items) > 2 {
		t.Errorf("expected at most 2 items on page 1, got %d", len(result.Items))
	}

	result2, err := ts.FlSvc.ListFiles("", false, 2, 2, "name", "asc")
	if err != nil {
		t.Fatalf("ListFiles page 2 failed: %v", err)
	}
	if result2.Page != 2 {
		t.Errorf("expected page 2, got %d", result2.Page)
	}
}

func TestGRPCDeleteFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	_, err = ts.FM.UploadFile("del.txt", []byte("delete me"))
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	err = ts.FM.DeleteFile("del.txt")
	if err != nil {
		t.Fatalf("DeleteFile failed: %v", err)
	}

	_, err = ts.FM.GetFileMetadata("del.txt")
	if err == nil {
		t.Errorf("expected error after deleting file, got nil")
	}
}

func TestGRPCRenameFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	_, err = ts.FM.UploadFile("old.txt", []byte("rename me"))
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	err = ts.FM.RenameFile("old.txt", "new.txt")
	if err != nil {
		t.Fatalf("RenameFile failed: %v", err)
	}

	_, err = ts.FM.GetFileMetadata("new.txt")
	if err != nil {
		t.Errorf("file not accessible at new path: %v", err)
	}

	_, err = ts.FM.GetFileMetadata("old.txt")
	if err == nil {
		t.Errorf("expected error accessing old path, got nil")
	}
}

func TestGRPCCreateDirectory(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	err = ts.DirSvc.CreateDirectory("mydir")
	if err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	if !ts.DirSvc.Exists("mydir") {
		t.Errorf("directory does not exist after creation")
	}
}

func TestGRPCDeleteDirectoryRecursive(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	ts.DirSvc.CreateDirectory("rmdir")
	ts.FM.UploadFile("rmdir/a.txt", []byte("a"))
	ts.FM.UploadFile("rmdir/b.txt", []byte("b"))

	err = ts.DirSvc.DeleteDirectory("rmdir", true)
	if err != nil {
		t.Fatalf("DeleteDirectory recursive failed: %v", err)
	}

	if ts.DirSvc.Exists("rmdir") {
		t.Errorf("directory still exists after recursive delete")
	}
	if ts.FM.Exists("rmdir/a.txt") {
		t.Errorf("file inside directory still exists after recursive delete")
	}
	if ts.FM.Exists("rmdir/b.txt") {
		t.Errorf("file inside directory still exists after recursive delete")
	}
}

func TestGRPCGetMetadata(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	data := []byte("metadata test")
	uploadMeta, err := ts.FM.UploadFile("meta.txt", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	meta, err := ts.FM.GetFileMetadata("meta.txt")
	if err != nil {
		t.Fatalf("GetFileMetadata failed: %v", err)
	}

	if meta.ID != uploadMeta.ID {
		t.Errorf("ID mismatch: expected %s, got %s", uploadMeta.ID, meta.ID)
	}
	if meta.Path != "meta.txt" {
		t.Errorf("path mismatch: expected meta.txt, got %s", meta.Path)
	}
	if meta.Name != "meta.txt" {
		t.Errorf("name mismatch: expected meta.txt, got %s", meta.Name)
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("size mismatch: expected %d, got %d", len(data), meta.Size)
	}
	if meta.Hash != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Errorf("hash mismatch")
	}
	if meta.StorageType != "local" {
		t.Errorf("storage type mismatch: expected local, got %s", meta.StorageType)
	}
	if meta.IsDeleted {
		t.Errorf("expected IsDeleted false")
	}
	if meta.CreatedAt == "" {
		t.Errorf("expected non-empty CreatedAt")
	}
	if meta.UpdatedAt == "" {
		t.Errorf("expected non-empty UpdatedAt")
	}
}

func TestGRPCGetDirectoryMetadata(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	err = ts.DirSvc.CreateDirectory("dirmeta")
	if err != nil {
		t.Fatalf("CreateDirectory failed: %v", err)
	}

	meta, err := ts.DirSvc.GetDirectoryMetadata("dirmeta")
	if err != nil {
		t.Fatalf("GetDirectoryMetadata failed: %v", err)
	}

	if meta.Path != "dirmeta" {
		t.Errorf("path mismatch: expected dirmeta, got %s", meta.Path)
	}
	if meta.Name != "dirmeta" {
		t.Errorf("name mismatch: expected dirmeta, got %s", meta.Name)
	}
	if meta.IsDeleted {
		t.Errorf("expected IsDeleted false")
	}
	if meta.CreatedAt == "" {
		t.Errorf("expected non-empty CreatedAt")
	}
	if meta.UpdatedAt == "" {
		t.Errorf("expected non-empty UpdatedAt")
	}
	if meta.ID == "" {
		t.Errorf("expected non-empty ID")
	}
}

func TestGRPCStreamingUpload(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	totalSize := int64(10 * 1024 * 1024)
	chunkSize := 1024 * 1024
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(data))

	sessionID, err := ts.TransferSvc.CreateUploadSession("stream_upload.bin", "stream_upload.bin", totalSize, "test-client", expectedHash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	var offset int64
	for offset < totalSize {
		end := offset + int64(chunkSize)
		if end > totalSize {
			end = totalSize
		}
		chunk := data[offset:end]
		if err := ts.TransferSvc.UploadChunk(sessionID, chunk, offset); err != nil {
			t.Fatalf("UploadChunk at offset %d failed: %v", offset, err)
		}
		offset = end
	}

	if err := ts.TransferSvc.CompleteUpload(sessionID); err != nil {
		t.Fatalf("CompleteUpload failed: %v", err)
	}

	downloaded, err := ts.FM.DownloadFile("stream_upload.bin")
	if err != nil {
		t.Fatalf("DownloadFile after streaming upload failed: %v", err)
	}

	if !bytes.Equal(downloaded, data) {
		t.Errorf("streaming upload content mismatch")
	}
}

func TestGRPCStreamingDownload(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	totalSize := int64(5 * 1024 * 1024)
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = ts.FM.UploadFile("stream_dl.bin", data)
	if err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	sessionID, err := ts.TransferSvc.CreateDownloadSession("stream_dl.bin", "test-client")
	if err != nil {
		t.Fatalf("CreateDownloadSession failed: %v", err)
	}

	chunkSize := 1024 * 1024
	var reassembled []byte
	var offset int64

	for offset < totalSize {
		remaining := totalSize - offset
		sz := int64(chunkSize)
		if remaining < sz {
			sz = remaining
		}
		chunk, err := ts.TransferSvc.DownloadChunk(sessionID, int(sz), offset)
		if err != nil {
			t.Fatalf("DownloadChunk at offset %d failed: %v", offset, err)
		}
		reassembled = append(reassembled, chunk...)
		offset += int64(len(chunk))
	}

	if err := ts.TransferSvc.CompleteDownload(sessionID); err != nil {
		t.Fatalf("CompleteDownload failed: %v", err)
	}

	if !bytes.Equal(reassembled, data) {
		t.Errorf("streaming download content mismatch")
	}
}

func TestGRPCUploadHashVerification(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	data := []byte("verify my hash")
	correctHash := fmt.Sprintf("%x", sha256.Sum256(data))
	wrongHash := utils.SHA256("wrong data")

	sessionID, err := ts.TransferSvc.CreateUploadSession("hashverify.txt", "hashverify.txt", int64(len(data)), "test-client", wrongHash)
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	if err := ts.TransferSvc.UploadChunk(sessionID, data, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	err = ts.TransferSvc.CompleteUpload(sessionID)
	if err == nil {
		t.Errorf("expected hash mismatch error, got nil")
	}

	sessionID2, err := ts.TransferSvc.CreateUploadSession("hashverify2.txt", "hashverify2.txt", int64(len(data)), "test-client", correctHash)
	if err != nil {
		t.Fatalf("CreateUploadSession with correct hash failed: %v", err)
	}

	if err := ts.TransferSvc.UploadChunk(sessionID2, data, 0); err != nil {
		t.Fatalf("UploadChunk failed: %v", err)
	}

	if err := ts.TransferSvc.CompleteUpload(sessionID2); err != nil {
		t.Fatalf("CompleteUpload with correct hash failed: %v", err)
	}
}

func TestGRPCUploadAbort(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer ts.Cleanup()

	sessionID, err := ts.TransferSvc.CreateUploadSession("abort.txt", "abort.txt", 1024, "test-client", "")
	if err != nil {
		t.Fatalf("CreateUploadSession failed: %v", err)
	}

	if err := ts.TransferSvc.AbortUpload(sessionID); err != nil {
		t.Fatalf("AbortUpload failed: %v", err)
	}

	_, err = ts.TransferSvc.GetUploadSession(sessionID)
	if err == nil {
		t.Errorf("expected error getting aborted session, got nil")
	}
}
