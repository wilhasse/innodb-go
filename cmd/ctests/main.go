package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var defaultTests = []string{
	"ib_cfg",
	"ib_compressed",
	"ib_cursor",
	"ib_mt_stress",
	"ib_perf1",
	"ib_status",
	"ib_test1",
	"ib_test2",
	"ib_test3",
	"ib_test5",
	"ib_types",
	"ib_update",
}

func main() {
	testsDirFlag := flag.String("tests-dir", "", "Path to C tests directory")
	prefixFlag := flag.String("prefix", "", "Install prefix for headers/libs")
	libDirFlag := flag.String("libdir", "", "Runtime library directory")
	testsFlag := flag.String("tests", "", "Comma-separated test binaries to run")
	allFlag := flag.Bool("all", false, "Run all default C tests")
	buildOnly := flag.Bool("build-only", false, "Only build the C tests")
	noBuild := flag.Bool("no-build", false, "Skip the build step")
	flag.Parse()

	testsDir := resolveTestsDir(*testsDirFlag)
	prefix := envOrDefault("INNODB_C_TESTS_PREFIX", *prefixFlag)
	libDir := resolveLibDir(prefix, *libDirFlag)

	if !*noBuild {
		if err := buildCTests(testsDir, prefix); err != nil {
			exitErr("build failed", err)
		}
	}

	if *buildOnly {
		return
	}

	tests := resolveTests(*testsFlag, *allFlag)
	if err := runCTests(testsDir, libDir, tests); err != nil {
		exitErr("test run failed", err)
	}
}

func resolveTestsDir(flagValue string) string {
	if flagValue != "" {
		return mustAbs(flagValue)
	}
	if env := os.Getenv("INNODB_C_TESTS_DIR"); env != "" {
		return mustAbs(env)
	}
	return mustAbs(filepath.Join("..", "oss-embedded-innodb", "tests"))
}

func resolveLibDir(prefix, flagValue string) string {
	if flagValue != "" {
		return mustAbs(flagValue)
	}
	if env := os.Getenv("INNODB_C_TESTS_LIBDIR"); env != "" {
		return mustAbs(env)
	}
	if prefix == "" {
		return ""
	}
	return mustAbs(filepath.Join(prefix, "lib"))
}

func resolveTests(flagValue string, all bool) []string {
	if flagValue != "" {
		parts := strings.Split(flagValue, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			name := strings.TrimSpace(part)
			if name != "" {
				out = append(out, name)
			}
		}
		return out
	}
	if all {
		return defaultTests
	}
	return defaultTests
}

func buildCTests(testsDir, prefix string) error {
	args := []string{"-f", "Makefile.examples"}
	if prefix != "" {
		args = append(args, "TOP="+prefix)
	}
	cmd := exec.Command("make", args...)
	cmd.Dir = testsDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	fmt.Printf("Running: make %s (dir %s)\n", strings.Join(args, " "), testsDir)
	return cmd.Run()
}

func runCTests(testsDir, libDir string, tests []string) error {
	for _, name := range tests {
		path := filepath.Join(testsDir, name)
		cmd := exec.Command(path)
		cmd.Dir = testsDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = withLibDir(os.Environ(), libDir)
		fmt.Printf("Running: %s\n", path)
		if err := cmd.Run(); err != nil {
			return err
		}
	}
	return nil
}

func withLibDir(env []string, libDir string) []string {
	if libDir == "" {
		return env
	}
	prefix := "LD_LIBRARY_PATH="
	for i, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			env[i] = prefix + libDir + string(os.PathListSeparator) + strings.TrimPrefix(kv, prefix)
			return env
		}
	}
	return append(env, prefix+libDir)
}

func envOrDefault(key, fallback string) string {
	if env := os.Getenv(key); env != "" {
		return env
	}
	return fallback
}

func mustAbs(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		exitErr("resolve path", err)
	}
	return abs
}

func exitErr(msg string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", msg, err)
	os.Exit(1)
}
