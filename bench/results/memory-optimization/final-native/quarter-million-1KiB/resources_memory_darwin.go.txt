package main

/*
#include <libproc.h>
#include <sys/resource.h>
#include <unistd.h>

static int bench_memory(unsigned long long *current, unsigned long long *peak) {
    struct rusage_info_v4 usage = {0};
    int status = proc_pid_rusage(getpid(), RUSAGE_INFO_V4, (rusage_info_t *)&usage);
    if (status == 0) {
        *current = usage.ri_phys_footprint;
        *peak = usage.ri_lifetime_max_phys_footprint;
    }
    return status;
}
*/
import "C"

func resourceFootprint() (uint64, uint64) {
	var current, peak C.ulonglong
	if C.bench_memory(&current, &peak) != 0 {
		panic("proc_pid_rusage memory measurement failed")
	}
	return uint64(current), uint64(peak)
}
