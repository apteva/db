//go:build !darwin

package main

// macOS-specific footprint accounting is unavailable on other platforms;
// report RSS instead and leave the explicitly named footprint fields zero.
func resourceFootprint() (uint64, uint64) { return 0, 0 }
