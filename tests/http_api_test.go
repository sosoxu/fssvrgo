package tests

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"testing"

	"github.com/sosoxu/fssvrgo/tests/testutil"
)

func doUpload(t *testing.T, ts *testutil.TestServer, filePath, fileName string, data []byte) *http.Response {
	t.Helper()
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	part.Write(data)
	if filePath != "" {
		writer.WriteField("path", filePath)
	}
	writer.Close()
	req, err := http.NewRequest("POST", ts.BaseURL+"/api/v1/files", body)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	return resp
}

func doChunkedCreate(t *testing.T, ts *testutil.TestServer, filePath, fileName string, totalSize int64, hash string) string {
	t.Helper()
	payload := map[string]interface{}{
		"file_path":  filePath,
		"file_name":  fileName,
		"total_size": totalSize,
	}
	if hash != "" {
		payload["hash"] = hash
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}
	req, err := http.NewRequest("POST", ts.BaseURL+"/api/v1/uploads", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 201, got %d: %s", resp.StatusCode, string(respBody))
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	sessionID, ok := result["session_id"].(string)
	if !ok {
		t.Fatalf("session_id not found in response")
	}
	return sessionID
}

func doChunkedUploadChunk(t *testing.T, ts *testutil.TestServer, sessionID string, data []byte, offset int64) {
	t.Helper()
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("data", "chunk")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	part.Write(data)
	writer.WriteField("offset", strconv.FormatInt(offset, 10))
	writer.Close()
	url := ts.BaseURL + "/api/v1/uploads/" + sessionID + "/chunk"
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d: %s", resp.StatusCode, string(respBody))
	}
}

func doChunkedComplete(t *testing.T, ts *testutil.TestServer, sessionID string) *http.Response {
	t.Helper()
	url := ts.BaseURL + "/api/v1/uploads/" + sessionID + "/complete"
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	return resp
}

func TestHealthCheck(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 200 with status ok database ok storage ok", func(t *testing.T) {
		// /ready (readiness probe) returns both database and storage status;
		// /health (liveness probe) only returns database status.
		resp, err := http.Get(ts.BaseURL + "/ready")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result["status"] != "ok" {
			t.Fatalf("expected status ok, got %v", result["status"])
		}
		if result["database"] != "ok" {
			t.Fatalf("expected database ok, got %v", result["database"])
		}
		if result["storage"] != "ok" {
			t.Fatalf("expected storage ok, got %v", result["storage"])
		}
	})
}

func TestUploadSmallFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 201 with metadata", func(t *testing.T) {
		data := make([]byte, 100)
		for i := range data {
			data[i] = byte(i % 256)
		}
		resp := doUpload(t, ts, "small.txt", "small.txt", data)
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected status 201, got %d: %s", resp.StatusCode, string(body))
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result["Path"] == nil {
			t.Fatal("expected Path in metadata")
		}
		if result["Name"] == nil {
			t.Fatal("expected Name in metadata")
		}
		if result["Size"] == nil {
			t.Fatal("expected Size in metadata")
		}
	})
}

func TestUploadMediumFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 201", func(t *testing.T) {
		data := bytes.Repeat([]byte("A"), 1024*1024)
		resp := doUpload(t, ts, "medium.dat", "medium.dat", data)
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected status 201, got %d: %s", resp.StatusCode, string(body))
		}
	})
}

func TestUploadNoFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 400", func(t *testing.T) {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		writer.Close()
		req, err := http.NewRequest("POST", ts.BaseURL+"/api/v1/files", body)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", resp.StatusCode)
		}
	})
}

func TestUploadInvalidFileName(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 400 for filename with ..", func(t *testing.T) {
		data := []byte("test content")
		resp := doUpload(t, ts, "", "..", data)
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", resp.StatusCode)
		}
	})
}

func TestUploadDuplicateFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("second upload overwrites (idempotent)", func(t *testing.T) {
		// Per design: idempotent upload — uploading to an existing path
		// overwrites the file and returns 201, not 500.
		data := []byte("duplicate content")
		resp1 := doUpload(t, ts, "dup.txt", "dup.txt", data)
		resp1.Body.Close()

		if resp1.StatusCode != http.StatusCreated {
			t.Fatalf("first upload expected status 201, got %d", resp1.StatusCode)
		}

		resp2 := doUpload(t, ts, "dup.txt", "dup.txt", data)
		defer resp2.Body.Close()

		if resp2.StatusCode != http.StatusCreated {
			t.Fatalf("second upload expected status 201 (idempotent overwrite), got %d", resp2.StatusCode)
		}
	})
}

func TestDownloadFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("content matches uploaded file", func(t *testing.T) {
		data := []byte("download test content")
		resp := doUpload(t, ts, "dl_test.txt", "dl_test.txt", data)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload failed with status %d", resp.StatusCode)
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files/dl_test.txt")
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		downloaded, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if !bytes.Equal(downloaded, data) {
			t.Fatalf("downloaded content does not match uploaded content")
		}
	})
}

func TestDownloadNonExistent(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 404", func(t *testing.T) {
		resp, err := http.Get(ts.BaseURL + "/api/v1/files/nonexistent.txt")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected status 404, got %d", resp.StatusCode)
		}
	})
}

func TestDownloadWithRange(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 206 with 100 bytes", func(t *testing.T) {
		content := make([]byte, 1024)
		for i := range content {
			content[i] = byte(i % 256)
		}
		resp := doUpload(t, ts, "range_test.txt", "range_test.txt", content)
		resp.Body.Close()

		req, err := http.NewRequest("GET", ts.BaseURL+"/api/v1/files/range_test.txt", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Range", "bytes=0-99")
		dlResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusPartialContent {
			t.Fatalf("expected status 206, got %d", dlResp.StatusCode)
		}

		body, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if len(body) != 100 {
			t.Fatalf("expected 100 bytes, got %d", len(body))
		}

		if !bytes.Equal(body, content[0:100]) {
			t.Fatalf("range content does not match expected bytes")
		}
	})
}

func TestDownloadWithRangeSuffix(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns last 100 bytes", func(t *testing.T) {
		content := make([]byte, 1024)
		for i := range content {
			content[i] = byte(i % 256)
		}
		resp := doUpload(t, ts, "range_suffix.txt", "range_suffix.txt", content)
		resp.Body.Close()

		req, err := http.NewRequest("GET", ts.BaseURL+"/api/v1/files/range_suffix.txt", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Range", "bytes=-100")
		dlResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusPartialContent {
			t.Fatalf("expected status 206, got %d", dlResp.StatusCode)
		}

		body, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if len(body) != 100 {
			t.Fatalf("expected 100 bytes, got %d", len(body))
		}

		if !bytes.Equal(body, content[924:1024]) {
			t.Fatalf("suffix range content does not match expected bytes")
		}
	})
}

func TestDownloadWithRangeMiddle(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 100 bytes from middle", func(t *testing.T) {
		content := make([]byte, 1024)
		for i := range content {
			content[i] = byte(i % 256)
		}
		resp := doUpload(t, ts, "range_middle.txt", "range_middle.txt", content)
		resp.Body.Close()

		req, err := http.NewRequest("GET", ts.BaseURL+"/api/v1/files/range_middle.txt", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Range", "bytes=100-199")
		dlResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusPartialContent {
			t.Fatalf("expected status 206, got %d", dlResp.StatusCode)
		}

		body, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if len(body) != 100 {
			t.Fatalf("expected 100 bytes, got %d", len(body))
		}

		if !bytes.Equal(body, content[100:200]) {
			t.Fatalf("middle range content does not match expected bytes")
		}
	})
}

func TestListFiles(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns uploaded files", func(t *testing.T) {
		files := []struct {
			path string
			data []byte
		}{
			{"list_a.txt", []byte("file a")},
			{"list_b.txt", []byte("file b")},
			{"list_c.txt", []byte("file c")},
		}
		for _, f := range files {
			resp := doUpload(t, ts, f.path, f.path, f.data)
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("upload failed for %s: status %d", f.path, resp.StatusCode)
			}
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files")
		if err != nil {
			t.Fatalf("failed to list files: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(dlResp.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		total, ok := result["Total"].(float64)
		if !ok {
			t.Fatal("Total not found in response")
		}
		if int(total) < 3 {
			t.Fatalf("expected at least 3 files, got %d", int(total))
		}

		items, ok := result["Items"].([]interface{})
		if !ok {
			t.Fatal("Items not found in response")
		}
		if len(items) < 3 {
			t.Fatalf("expected at least 3 items, got %d", len(items))
		}
	})
}

func TestListFilesPagination(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("respects page and page_size params", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			name := fmt.Sprintf("page_file_%d.txt", i)
			resp := doUpload(t, ts, name, name, []byte(name))
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("upload failed for %s: status %d", name, resp.StatusCode)
			}
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files?page=1&page_size=2")
		if err != nil {
			t.Fatalf("failed to list files: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(dlResp.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		pageSize, ok := result["PageSize"].(float64)
		if !ok {
			t.Fatal("PageSize not found in response")
		}
		if int(pageSize) != 2 {
			t.Fatalf("expected PageSize 2, got %d", int(pageSize))
		}

		items, ok := result["Items"].([]interface{})
		if !ok {
			t.Fatal("Items not found in response")
		}
		if len(items) > 2 {
			t.Fatalf("expected at most 2 items on page, got %d", len(items))
		}

		total, ok := result["Total"].(float64)
		if !ok {
			t.Fatal("Total not found in response")
		}
		if int(total) < 5 {
			t.Fatalf("expected total at least 5, got %d", int(total))
		}
	})
}

func TestListFilesSortBy(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("sorts by name desc", func(t *testing.T) {
		names := []string{"alpha.txt", "beta.txt", "gamma.txt"}
		for _, name := range names {
			resp := doUpload(t, ts, name, name, []byte(name))
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("upload failed for %s: status %d", name, resp.StatusCode)
			}
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files?sort_by=name&sort_order=desc")
		if err != nil {
			t.Fatalf("failed to list files: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(dlResp.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		items, ok := result["Items"].([]interface{})
		if !ok {
			t.Fatal("Items not found in response")
		}

		var itemNames []string
		for _, item := range items {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if name, ok := m["Name"].(string); ok {
				itemNames = append(itemNames, name)
			}
		}

		found := false
		for i := 0; i < len(itemNames)-1; i++ {
			for _, target := range names {
				if itemNames[i] == target {
					found = true
					break
				}
			}
			if found {
				break
			}
		}

		sorted := true
		for i := 0; i < len(itemNames)-1; i++ {
			if itemNames[i] < itemNames[i+1] {
				sorted = false
				break
			}
		}

		if !sorted && len(itemNames) > 1 {
			t.Fatalf("expected items sorted by name desc, got %v", itemNames)
		}

		_ = found
	})
}

func TestDeleteFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 200 and file no longer downloadable", func(t *testing.T) {
		data := []byte("delete test content")
		resp := doUpload(t, ts, "delete_test.txt", "delete_test.txt", data)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload failed with status %d", resp.StatusCode)
		}

		req, err := http.NewRequest("DELETE", ts.BaseURL+"/api/v1/files/delete_test.txt", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		delResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to delete: %v", err)
		}
		defer delResp.Body.Close()

		if delResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", delResp.StatusCode)
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files/delete_test.txt")
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected status 404 after delete, got %d", dlResp.StatusCode)
		}
	})
}

func TestDeleteNonExistent(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 500", func(t *testing.T) {
		req, err := http.NewRequest("DELETE", ts.BaseURL+"/api/v1/files/nonexistent.txt", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to delete: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected status 500, got %d", resp.StatusCode)
		}
	})
}

func TestRenameFile(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 200 and file accessible at new name", func(t *testing.T) {
		data := []byte("rename test content")
		resp := doUpload(t, ts, "rename_test.txt", "rename_test.txt", data)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload failed with status %d", resp.StatusCode)
		}

		renamePayload := map[string]string{"new_name": "renamed.txt"}
		renameBody, _ := json.Marshal(renamePayload)
		req, err := http.NewRequest("PATCH", ts.BaseURL+"/api/v1/files/rename_test.txt", bytes.NewReader(renameBody))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		renameResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to rename: %v", err)
		}
		defer renameResp.Body.Close()

		if renameResp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(renameResp.Body)
			t.Fatalf("expected status 200, got %d: %s", renameResp.StatusCode, string(body))
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files/renamed.txt")
		if err != nil {
			t.Fatalf("failed to download renamed file: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200 for renamed file, got %d", dlResp.StatusCode)
		}

		downloaded, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if !bytes.Equal(downloaded, data) {
			t.Fatalf("renamed file content does not match")
		}

		oldResp, err := http.Get(ts.BaseURL + "/api/v1/files/rename_test.txt")
		if err != nil {
			t.Fatalf("failed to download old file: %v", err)
		}
		defer oldResp.Body.Close()

		if oldResp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected status 404 for old file name, got %d", oldResp.StatusCode)
		}
	})
}

func TestRenameInvalidName(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 400 for rename to ..", func(t *testing.T) {
		data := []byte("invalid rename test")
		resp := doUpload(t, ts, "invalid_rename.txt", "invalid_rename.txt", data)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload failed with status %d", resp.StatusCode)
		}

		renamePayload := map[string]string{"new_name": ".."}
		renameBody, _ := json.Marshal(renamePayload)
		req, err := http.NewRequest("PATCH", ts.BaseURL+"/api/v1/files/invalid_rename.txt", bytes.NewReader(renameBody))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		renameResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to rename: %v", err)
		}
		defer renameResp.Body.Close()

		if renameResp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", renameResp.StatusCode)
		}
	})
}

func TestCreateDirectory(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 201", func(t *testing.T) {
		payload := map[string]string{"path": "testdir"}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequest("POST", ts.BaseURL+"/api/v1/directories", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected status 201, got %d", resp.StatusCode)
		}
	})
}

func TestCreateDirectoryDuplicate(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 500", func(t *testing.T) {
		payload := map[string]string{"path": "dupdir"}
		body, _ := json.Marshal(payload)

		req1, err := http.NewRequest("POST", ts.BaseURL+"/api/v1/directories", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req1.Header.Set("Content-Type", "application/json")
		resp1, err := http.DefaultClient.Do(req1)
		if err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
		resp1.Body.Close()

		if resp1.StatusCode != http.StatusCreated {
			t.Fatalf("first create expected status 201, got %d", resp1.StatusCode)
		}

		body2, _ := json.Marshal(payload)
		req2, err := http.NewRequest("POST", ts.BaseURL+"/api/v1/directories", bytes.NewReader(body2))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req2.Header.Set("Content-Type", "application/json")
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
		defer resp2.Body.Close()

		if resp2.StatusCode != http.StatusInternalServerError {
			t.Fatalf("second create expected status 500, got %d", resp2.StatusCode)
		}
	})
}

func TestGetFileMetadata(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns type=file", func(t *testing.T) {
		data := []byte("metadata test content")
		resp := doUpload(t, ts, "meta_file.txt", "meta_file.txt", data)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload failed with status %d", resp.StatusCode)
		}

		metaResp, err := http.Get(ts.BaseURL + "/api/v1/metadata/meta_file.txt")
		if err != nil {
			t.Fatalf("failed to get metadata: %v", err)
		}
		defer metaResp.Body.Close()

		if metaResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", metaResp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(metaResp.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result["type"] != "file" {
			t.Fatalf("expected type file, got %v", result["type"])
		}
	})
}

func TestGetDirectoryMetadata(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns type=directory", func(t *testing.T) {
		payload := map[string]string{"path": "metadir"}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequest("POST", ts.BaseURL+"/api/v1/directories", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		dirResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
		dirResp.Body.Close()

		if dirResp.StatusCode != http.StatusCreated {
			t.Fatalf("create directory failed with status %d", dirResp.StatusCode)
		}

		metaResp, err := http.Get(ts.BaseURL + "/api/v1/metadata/metadir")
		if err != nil {
			t.Fatalf("failed to get metadata: %v", err)
		}
		defer metaResp.Body.Close()

		if metaResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", metaResp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(metaResp.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result["type"] != "directory" {
			t.Fatalf("expected type directory, got %v", result["type"])
		}
	})
}

func TestGetNonExistentMetadata(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 404", func(t *testing.T) {
		resp, err := http.Get(ts.BaseURL + "/api/v1/metadata/nonexistent")
		if err != nil {
			t.Fatalf("failed to get metadata: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected status 404, got %d", resp.StatusCode)
		}
	})
}

func TestChunkedUploadSmall(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("create session upload chunk complete verify", func(t *testing.T) {
		content := []byte("chunked small content")
		sessionID := doChunkedCreate(t, ts, "chunked_small.txt", "chunked_small.txt", int64(len(content)), "")
		doChunkedUploadChunk(t, ts, sessionID, content, 0)

		completeResp := doChunkedComplete(t, ts, sessionID)
		defer completeResp.Body.Close()

		if completeResp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(completeResp.Body)
			t.Fatalf("expected status 200, got %d: %s", completeResp.StatusCode, string(body))
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files/chunked_small.txt")
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		downloaded, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if !bytes.Equal(downloaded, content) {
			t.Fatalf("downloaded content does not match")
		}
	})
}

func TestChunkedUploadMedium(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("create session upload 256KB chunks complete verify", func(t *testing.T) {
		totalSize := 1024 * 1024
		content := make([]byte, totalSize)
		for i := range content {
			content[i] = byte(i % 256)
		}
		chunkSize := 256 * 1024

		sessionID := doChunkedCreate(t, ts, "chunked_medium.dat", "chunked_medium.dat", int64(totalSize), "")

		offset := int64(0)
		for offset < int64(totalSize) {
			end := offset + int64(chunkSize)
			if end > int64(totalSize) {
				end = int64(totalSize)
			}
			doChunkedUploadChunk(t, ts, sessionID, content[offset:end], offset)
			offset = end
		}

		completeResp := doChunkedComplete(t, ts, sessionID)
		defer completeResp.Body.Close()

		if completeResp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(completeResp.Body)
			t.Fatalf("expected status 200, got %d: %s", completeResp.StatusCode, string(body))
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files/chunked_medium.dat")
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		downloaded, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if !bytes.Equal(downloaded, content) {
			t.Fatalf("downloaded content does not match")
		}
	})
}

func TestChunkedUploadLarge(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("create session upload 1MB chunks complete verify content", func(t *testing.T) {
		totalSize := 10 * 1024 * 1024
		content := make([]byte, totalSize)
		for i := range content {
			content[i] = byte(i % 256)
		}
		chunkSize := 1024 * 1024

		sessionID := doChunkedCreate(t, ts, "chunked_large.dat", "chunked_large.dat", int64(totalSize), "")

		offset := int64(0)
		for offset < int64(totalSize) {
			end := offset + int64(chunkSize)
			if end > int64(totalSize) {
				end = int64(totalSize)
			}
			doChunkedUploadChunk(t, ts, sessionID, content[offset:end], offset)
			offset = end
		}

		completeResp := doChunkedComplete(t, ts, sessionID)
		defer completeResp.Body.Close()

		if completeResp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(completeResp.Body)
			t.Fatalf("expected status 200, got %d: %s", completeResp.StatusCode, string(body))
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files/chunked_large.dat")
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		downloaded, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if len(downloaded) != totalSize {
			t.Fatalf("expected %d bytes, got %d", totalSize, len(downloaded))
		}

		if !bytes.Equal(downloaded, content) {
			t.Fatalf("downloaded content does not match")
		}
	})
}

func TestChunkedUploadWithHashVerification(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("upload with SHA256 hash complete verifies hash", func(t *testing.T) {
		content := []byte("hash verification test content")
		hash := fmt.Sprintf("%x", sha256.Sum256(content))

		sessionID := doChunkedCreate(t, ts, "chunked_hash.txt", "chunked_hash.txt", int64(len(content)), hash)
		doChunkedUploadChunk(t, ts, sessionID, content, 0)

		completeResp := doChunkedComplete(t, ts, sessionID)
		defer completeResp.Body.Close()

		if completeResp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(completeResp.Body)
			t.Fatalf("expected status 200, got %d: %s", completeResp.StatusCode, string(body))
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files/chunked_hash.txt")
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		downloaded, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if !bytes.Equal(downloaded, content) {
			t.Fatalf("downloaded content does not match")
		}
	})
}

func TestChunkedUploadAbort(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("create session upload chunk abort session gone", func(t *testing.T) {
		content := []byte("abort test content")
		sessionID := doChunkedCreate(t, ts, "chunked_abort.txt", "chunked_abort.txt", int64(len(content)), "")
		doChunkedUploadChunk(t, ts, sessionID, content, 0)

		req, err := http.NewRequest("DELETE", ts.BaseURL+"/api/v1/uploads/"+sessionID, nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		abortResp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to abort: %v", err)
		}
		defer abortResp.Body.Close()

		if abortResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", abortResp.StatusCode)
		}

		progressResp, err := http.Get(ts.BaseURL + "/api/v1/uploads/" + sessionID + "/progress")
		if err != nil {
			t.Fatalf("failed to get progress: %v", err)
		}
		defer progressResp.Body.Close()

		var progress map[string]interface{}
		if err := json.NewDecoder(progressResp.Body).Decode(&progress); err != nil {
			t.Fatalf("failed to decode progress response: %v", err)
		}

		uploadedBytes, ok := progress["uploaded_bytes"].(float64)
		if !ok {
			t.Fatal("uploaded_bytes not found in progress response")
		}

		if int64(uploadedBytes) != 0 {
			t.Fatalf("expected uploaded_bytes 0 after abort, got %d", int64(uploadedBytes))
		}
	})
}

func TestChunkedUploadProgress(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("create session upload chunk check progress", func(t *testing.T) {
		content := make([]byte, 500)
		for i := range content {
			content[i] = byte(i % 256)
		}
		sessionID := doChunkedCreate(t, ts, "chunked_progress.txt", "chunked_progress.txt", 1000, "")
		doChunkedUploadChunk(t, ts, sessionID, content, 0)

		progressResp, err := http.Get(ts.BaseURL + "/api/v1/uploads/" + sessionID + "/progress")
		if err != nil {
			t.Fatalf("failed to get progress: %v", err)
		}
		defer progressResp.Body.Close()

		if progressResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", progressResp.StatusCode)
		}

		var progress map[string]interface{}
		if err := json.NewDecoder(progressResp.Body).Decode(&progress); err != nil {
			t.Fatalf("failed to decode progress response: %v", err)
		}

		uploadedBytes, ok := progress["uploaded_bytes"].(float64)
		if !ok {
			t.Fatal("uploaded_bytes not found in progress response")
		}

		if int64(uploadedBytes) != 500 {
			t.Fatalf("expected uploaded_bytes 500, got %d", int64(uploadedBytes))
		}
	})
}

func TestChunkedUploadInvalidFileName(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 400 for filename with ..", func(t *testing.T) {
		payload := map[string]interface{}{
			"file_path":  "invalid.txt",
			"file_name":  "..",
			"total_size": 100,
		}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequest("POST", ts.BaseURL+"/api/v1/uploads", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", resp.StatusCode)
		}
	})
}

func TestMetrics(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns 200 with version and uptime", func(t *testing.T) {
		resp, err := http.Get(ts.BaseURL + "/metrics")
		if err != nil {
			t.Fatalf("failed to get metrics: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", resp.StatusCode)
		}

		var result map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result["version"] == nil {
			t.Fatal("expected version in metrics response")
		}
		if result["uptime"] == nil {
			t.Fatal("expected uptime in metrics response")
		}
	})
}

func TestCORSPreflight(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("returns correct headers", func(t *testing.T) {
		req, err := http.NewRequest("OPTIONS", ts.BaseURL+"/api/v1/health", nil)
		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}
		req.Header.Set("Origin", "http://example.com")
		req.Header.Set("Access-Control-Request-Method", "POST")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("expected status 204, got %d", resp.StatusCode)
		}

		allowOrigin := resp.Header.Get("Access-Control-Allow-Origin")
		if allowOrigin != "*" && allowOrigin != "http://example.com" {
			t.Fatalf("expected Access-Control-Allow-Origin * or http://example.com, got %s", allowOrigin)
		}

		allowMethods := resp.Header.Get("Access-Control-Allow-Methods")
		if allowMethods == "" {
			t.Fatal("expected Access-Control-Allow-Methods header")
		}

		allowHeaders := resp.Header.Get("Access-Control-Allow-Headers")
		if allowHeaders == "" {
			t.Fatal("expected Access-Control-Allow-Headers header")
		}

		maxAge := resp.Header.Get("Access-Control-Max-Age")
		if maxAge != "86400" {
			t.Fatalf("expected Access-Control-Max-Age 86400, got %s", maxAge)
		}
	})
}

func TestLargeFileStreamingDownload(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("upload 5MB download and verify content matches", func(t *testing.T) {
		size := 5 * 1024 * 1024
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i % 256)
		}

		resp := doUpload(t, ts, "large_5mb.dat", "large_5mb.dat", content)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload failed with status %d", resp.StatusCode)
		}

		dlResp, err := http.Get(ts.BaseURL + "/api/v1/files/large_5mb.dat")
		if err != nil {
			t.Fatalf("failed to download: %v", err)
		}
		defer dlResp.Body.Close()

		if dlResp.StatusCode != http.StatusOK {
			t.Fatalf("expected status 200, got %d", dlResp.StatusCode)
		}

		downloaded, err := io.ReadAll(dlResp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %v", err)
		}

		if len(downloaded) != size {
			t.Fatalf("expected %d bytes, got %d", size, len(downloaded))
		}

		if !bytes.Equal(downloaded, content) {
			t.Fatalf("downloaded content does not match uploaded content")
		}
	})
}

func TestLargeFileRangeDownload(t *testing.T) {
	ts, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Cleanup()

	t.Run("upload 5MB download in 1MB chunks reassemble and verify", func(t *testing.T) {
		size := 5 * 1024 * 1024
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i % 256)
		}

		resp := doUpload(t, ts, "range_5mb.dat", "range_5mb.dat", content)
		resp.Body.Close()

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload failed with status %d", resp.StatusCode)
		}

		chunkSize := 1024 * 1024
		var reassembled []byte

		for offset := 0; offset < size; offset += chunkSize {
			end := offset + chunkSize - 1
			if end >= size {
				end = size - 1
			}

			req, err := http.NewRequest("GET", ts.BaseURL+"/api/v1/files/range_5mb.dat", nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))

			dlResp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("failed to download range: %v", err)
			}

			if dlResp.StatusCode != http.StatusPartialContent {
				dlResp.Body.Close()
				t.Fatalf("expected status 206, got %d", dlResp.StatusCode)
			}

			chunk, err := io.ReadAll(dlResp.Body)
			dlResp.Body.Close()
			if err != nil {
				t.Fatalf("failed to read chunk: %v", err)
			}

			reassembled = append(reassembled, chunk...)
		}

		if len(reassembled) != size {
			t.Fatalf("expected %d bytes reassembled, got %d", size, len(reassembled))
		}

		if !bytes.Equal(reassembled, content) {
			t.Fatalf("reassembled content does not match original")
		}
	})
}
