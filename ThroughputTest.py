import subprocess, time

tool   = "bazel-bin/service/tools/kv/api_tools/kv_service_tools"
config = "service/tools/config/interface/service.config"
N      = 1000

start = time.time()
for i in range(N):
    result = subprocess.run(
        [tool, config, "set", f"key{i}", f"value{i}"],
        capture_output=True, text=True
    )
elapsed = time.time() - start

print(f"Transactions : {N}")
print(f"Total time   : {elapsed:.2f} sec")
print(f"Throughput   : {N/elapsed:.2f} ops/sec")
print(f"Avg latency  : {elapsed/N*1000:.2f} ms/op")