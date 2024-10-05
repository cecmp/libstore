package libstore_test

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/cecmp/libstore"
)

func TestDefaultFileOps(t *testing.T) {
	var fileOps libstore.Ops
	fileOps, err := libstore.NewFileOps(".")
	if err != nil {
		t.Fatal(err)
	}
	fileName := "testfile.txt"
	err = fileOps.Create(context.TODO(), fileName)
	if err != nil {
		t.Errorf("Error creating file: %v", err)
	}
	defer func() {
		err := fileOps.Delete(context.TODO(), fileName)
		if err == nil {
			t.Errorf("Error deleting file: %v", err)
		}
	}()

	_, err = fileOps.Read(context.TODO(), fileName)
	if err == nil {
		t.Error("Expected an error when reading an empty file")
	}

	line1 := "Line 1"
	line2 := "Line 2"
	line3 := "Line 3"
	multipleEntries := []string{line1, line2, line3}
	for _, c := range multipleEntries {
		err = fileOps.Put(context.TODO(), fileName, []byte(c))
		if err != nil {
			t.Errorf("Error appending to file: %v", err)
		}
	}

	readContent, err := fileOps.ReadAll(context.TODO(), fileName)
	if err != nil {
		t.Errorf("Error reading file: %v", err)
	}
	if len(readContent) != len(multipleEntries) {
		t.Error("Content len mismatch. Expected:", len(multipleEntries), "Got", len(readContent))
	}
	for i, c := range readContent {
		if string(c) != string(multipleEntries[i]) {
			t.Error("Content mismatch. Expected:", multipleEntries, "Got:", string(c))
		}
	}

	readLineContent, err := fileOps.Read(context.TODO(), fileName)
	if err != nil {
		t.Errorf("Error reading last line: %v", err)
	}
	if string(readLineContent) != line3 {
		t.Error("Last line content mismatch. Expected:", line3, "Got:", readLineContent)
	}

	err = fileOps.Delete(context.TODO(), fileName)
	if err != nil {
		t.Errorf("Error deleting file: %v", err)
	}
}

func TestWalkDir(t *testing.T) {
	var fileOps libstore.Ops
	testDir := "testdir"
	fileOps, err := libstore.NewFileOps(testDir)
	if err != nil {
		t.Fatal(err)
	}
	fileName1 := "file1.txt"
	fileName2 := "file2.txt"

	defer func() {
		err := os.RemoveAll(testDir)
		if err != nil {
			t.Fatalf("Error removing test directory: %v", err)
		}
	}()

	err = fileOps.Create(context.TODO(), fileName1)
	if err != nil {
		t.Fatalf("Error creating file1: %v", err)
	}
	defer func() {
		err := fileOps.Delete(context.TODO(), fileName1)
		if err != nil {
			t.Fatalf("Error deleting file1: %v", err)
		}
	}()

	err = fileOps.Create(context.TODO(), fileName2)
	if err != nil {
		t.Fatalf("Error creating file2: %v", err)
	}
	defer func() {
		err := fileOps.Delete(context.TODO(), fileName2)
		if err != nil {
			t.Fatalf("Error deleting file2: %v", err)
		}
	}()
	err = fileOps.Create(context.TODO(), fileName1)
	if err == nil {
		t.Fatalf("Exceted an error, file should already exists: %v", err)
	}
	// Test WalkDir
	var foundFiles []string
	foundFiles, err = fileOps.List(context.TODO())
	if err != nil {
		t.Fatalf("Error walking directory: %v", err)
	}

	// Assert the result
	expectedFiles := []string{fileName1, fileName2}
	if !reflect.DeepEqual(foundFiles, expectedFiles) {
		t.Errorf("Unexpected files found. Expected: %v, Got: %v", expectedFiles, foundFiles)
	}
}
