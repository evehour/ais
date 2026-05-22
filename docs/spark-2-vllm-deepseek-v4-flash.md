# DeepSeek-V4-Flash 듀얼 DGX Spark 설치 매뉴얼

> **상태**: 배포 완료, 서비스 가동 중  
> **구성**: DGX Spark (GB10) × 2 · ConnectX-7 200G RoCE · vLLM TP=2 · no-Ray  
> **기준일**: 2026-05-22

---

## 목차

1. [환경 및 클러스터 구성](#1-환경-및-클러스터-구성)
2. [아키텍처 개요](#2-아키텍처-개요)
3. [사전 준비 — SSH · 저장소 clone](#3-사전-준비--ssh--저장소-clone)
4. [커스터마이징 패치 3종](#4-커스터마이징-패치-3종)
5. [설정 파일 작성](#5-설정-파일-작성)
6. [이미지 빌드 및 노드2 배포](#6-이미지-빌드-및-노드2-배포)
7. [모델 배치](#7-모델-배치)
8. [vLLM 구동 · 종료 · 상태 확인](#8-vllm-구동--종료--상태-확인)
9. [동작 검증](#9-동작-검증)
10. [성능 측정 결과](#10-성능-측정-결과)
11. [운영 팁 및 주의사항](#11-운영-팁-및-주의사항)
12. [부록 (Diff, 추가 파일)](#12-부록)
---

## 1. 환경 및 클러스터 구성

| 구분 | 노드1 (head · rank 0) | 노드2 (worker · rank 1) |
|---|---|---|
| 호스트명 | aitopatom-bac8 | aitopatom-c8c8 |
| 외부 IP | 192.168.1.12 (enP7s7) | 192.168.1.13 (enP7s7) |
| 클러스터 IP | 192.168.200.12 (enp1s0f1np1) | 192.168.200.13 (enp1s0f1np1) |
| 역할 | API 서버 :8000, 클러스터 제어 | headless 워커, 텐서 병렬 연산 |
| SSH 계정 | myspark | myspark |
| 모델 경로 | /home/myspark/models/DeepSeek-V4-Flash | /home/myspark/models/DeepSeek-V4-Flash |

**공통 사양**

| 항목 | 값 |
|---|---|
| 하드웨어 | NVIDIA DGX Spark (GB10) |
| OS / 커널 | Ubuntu 24.04.4 / 6.17.0-nvidia |
| GPU / 드라이버 | NVIDIA GB10 / 580.142 |
| Docker / Python | 29.2.1 / 3.12.3 |
| 인터커넥트 | ConnectX-7 200G (2×NDR), RoCE — 실측 ~185 Gb/s |
| RDMA 디바이스 | `rocep1s0f1`, `roceP2p1s0f1` (물리 케이블 1개가 OS에 2개로 열거) |
| 모델 | DeepSeek-V4-Flash (~149 GB, safetensors 46샤드) |

**설치 앱**
https://www.gigabyte.com/kr/AI-TOP-PC/GIGABYTE-AI-TOP-ATOM/support
1. `AI TOP ATOM SOC/EC/PD Firmware - 2.144.9` 수행 함
2. `GIGABYTE AI TOP Utility 4.2.1: AI TOP ATOM Support Ubuntu 24.04.4 with Linux Kernel 6.17-1008-nvidia` 설치 후 초기 구동 함
   - 이거 하고나니까 conda가 .bashrc에 등록되서 실행되서 좀 편했음
4. `NVIDIA Dashboard`에서 업데이트 수행 함

---

## 2. 아키텍처 개요

```
노드1 (head, rank 0) — 192.168.1.12 / 192.168.200.12
  ├─ OpenAI API 서버 :8000   ◄─── 외부 요청 수신
  ├─ run-recipe.sh / launch-cluster.sh (클러스터 제어)
  ├─ .env · recipe
  └─ SSH ─────────── 노드2 제어 (기동/종료) ──────────►

노드2 (worker, rank 1) — 192.168.1.13 / 192.168.200.13
  └─ headless 워커 (API 서버 없음, 텐서 병렬 연산만)

클러스터 통신: 192.168.200.x (NCCL over RoCE)
```

- API 서버는 **노드1에만** 존재한다. 노드2는 `--headless`로 기동되며 직접 건드릴 필요가 없다.
- 모든 start/stop 제어는 노드1에서만 수행하며, 노드1이 SSH로 노드2를 원격 제어한다.
- TP=2 구성으로 모델이 두 노드에 절반씩 분산(노드당 약 75 GiB GPU 메모리).

---

## 3. 사전 준비 — SSH · 저장소 clone

### 전체 설치 흐름

| # | 단계 | 수행 노드 |
|---|---|---|
| 1 | SSH 키 생성 및 양방향 passwordless SSH 구성 | 노드1 + 노드2 |
| 2 | 저장소 clone (spark-vllm-docker, recipe) | 노드1 |
| 3 | 커스터마이징 패치 3종 적용 | 노드1 |
| 4 | .env 및 recipe 작성 | 노드1 |
| 5 | 컨테이너 이미지 빌드 + 노드2 자동 배포 | 노드1 (→ 노드2 자동) |
| 6 | 모델 배치 (양 노드 동일 경로) | 노드1 + 노드2 |
| 7 | vLLM 구동 | 노드1 (→ 노드2 SSH 자동 기동) |

### 노드2 — SSH 키 등록

노드2에서 직접 해야 할 작업은 SSH 키 등록과 모델 배치뿐이다. 이후 모든 원격 제어는 노드1이 자동 처리한다.

```bash
# 노드2에서 실행
mkdir -p ~/.ssh && chmod 700 ~/.ssh
[ -f ~/.ssh/id_ed25519 ] || ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519

# 노드1의 공개키를 authorized_keys에 추가 (노드1의 ~/.ssh/id_ed25519.pub 내용 붙여넣기)
echo "노드1_공개키_내용" >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# 노드1 클러스터 IP를 known_hosts에 등록
ssh-keyscan -H 192.168.200.12 >> ~/.ssh/known_hosts
```

### 노드1 — SSH 키 등록 및 저장소 clone

```bash
# 노드1에서 실행
mkdir -p ~/.ssh && chmod 700 ~/.ssh
[ -f ~/.ssh/id_ed25519 ] || ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519

# 노드2 공개키 등록 (노드2의 ~/.ssh/id_ed25519.pub 내용 붙여넣기)
echo "노드2_공개키_내용" >> ~/.ssh/authorized_keys

# 노드2 known_hosts 등록
ssh-keyscan -H 192.168.200.13 >> ~/.ssh/known_hosts

# 검증 — 비밀번호 없이 성공해야 함
ssh 192.168.200.13 hostname
```

```bash
# 저장소 clone
cd /home/myspark
git clone --depth 1 https://github.com/eugr/spark-vllm-docker.git
git clone --depth 1 https://github.com/tonyd2wild/deepseek-v4-flash-dual-spark-recipe.git
```

> ⚠️ **passwordless SSH 필수** — 노드1→노드2 passwordless SSH가 살아 있어야 start/stop이 정상 동작한다. 구성 전에 반드시 `ssh 192.168.200.13 hostname`으로 검증할 것.

---

## 4. 커스터마이징 패치 3종

원본 `eugr/spark-vllm-docker`는 vLLM 저장소 URL이 `vllm-project/vllm`으로 하드코딩되어 있어 DeepSeek V4를 지원하는 `jasl/vllm` 포크를 빌드할 수 없다. 아래 패치 3종을 노드1의 `spark-vllm-docker` 디렉터리에 적용해야 한다.

| 파일 이름 (부록 참조) | 대상 | 내용 |
|---|---|---|
| `spark_patch.py` | `Dockerfile`, `build-and-copy.sh` | `--vllm-repo` 인자 지원 추가. Dockerfile에 `ARG VLLM_REPO` 파라미터화, `build-and-copy.sh`에 `--vllm-repo <url>` 파싱 추가 |
| `spark_patch2.py` | `Dockerfile` | 강제 푸시로 orphan된 커밋도 받도록 `git fetch origin <REF>` + `checkout FETCH_HEAD` 방식으로 변경 |
| `spark_patch3.py` | `launch-cluster.sh` | `~/.tilelang`(TileLang 커널), `~/.nv`(CUDA NVRTC/PTX JIT 캐시)를 컨테이너에 bind-mount 추가. 미적용 시 재시작마다 커널 캐시 소실 → 추론 중 런타임 재컴파일 → 60초 freeze 발생 |

**패치 후 JIT 캐시 마운트 전체 목록**

| 컨테이너 경로 | 호스트 경로 | 비고 |
|---|---|---|
| `/root/.cache/vllm` | `~/.cache/vllm` | torch.compile 그래프 (원본 포함) |
| `/root/.triton` | `~/.triton` | Triton 커널 (원본 포함) |
| `/root/.cache/flashinfer` | `~/.cache/flashinfer` | FlashInfer 커널 (원본 포함) |
| `/root/.tilelang` | `~/.tilelang` | TileLang 커널 (**spark_patch3.py로 추가**) |
| `/root/.nv` | `~/.nv` | CUDA NVRTC/PTX JIT 캐시 (**spark_patch3.py로 추가**) |

패치 적용 후 실행 중 컨테이너의 기존 캐시를 호스트로 미리 복사해 다음 재시작이 warm 상태로 시작하게 한다.

```bash
docker cp vllm_deepseek_v4_flash:/root/.tilelang ~/.tilelang
docker cp vllm_deepseek_v4_flash:/root/.nv ~/.nv
```

---

## 5. 설정 파일 작성

### 클러스터 환경변수 — `/home/myspark/spark-vllm-docker/.env`

```ini
CLUSTER_NODES=192.168.200.12,192.168.200.13
COPY_HOSTS=192.168.200.13
LOCAL_IP=192.168.200.12
ETH_IF=enp1s0f1np1
IB_IF=rocep1s0f1,roceP2p1s0f1
MASTER_PORT=29501
CONTAINER_NAME=vllm_deepseek_v4_flash
```

### Recipe — `/home/myspark/spark-vllm-docker/recipes/deepseek-v4-flash.yaml`

```yaml
recipe_version: "1"
name: DeepSeek V4 Flash
container: vllm-node-dsv4
cluster_only: true
build_args:
  - --vllm-repo
  - https://github.com/jasl/vllm.git
  - --vllm-ref
  - b1c97ff068b23858a4759394f8d6c858d822c957
  - --rebuild-vllm
defaults:
  port: 8000
  host: 0.0.0.0
  tensor_parallel: 2
  pipeline_parallel: 1
  gpu_memory_utilization: 0.8
  max_model_len: 200000
  max_num_batched_tokens: 16384
  max_num_seqs: 4
  block_size: 256
  served_model_name: deepseek-v4-flash
env:
  TORCH_CUDA_ARCH_LIST: 12.1a
  VLLM_ALLOW_LONG_MAX_MODEL_LEN: "1"
  VLLM_TRITON_MLA_SPARSE: "1"
  FLASHINFER_DISABLE_VERSION_CHECK: "1"
  TILELANG_CLEANUP_TEMP_FILES: "1"
  DG_JIT_USE_NVRTC: "0"
  DG_JIT_NVCC_COMPILER: /usr/local/cuda/bin/nvcc
  NCCL_IB_DISABLE: "0"
  NCCL_DEBUG: WARN
  HF_HUB_OFFLINE: "1"
  TRANSFORMERS_OFFLINE: "1"
  OMP_NUM_THREADS: "8"
  VLLM_USE_FLASHINFER_SAMPLER: "1"
  CUDA_CACHE_MAXSIZE: "4294967296"
command: |
  vllm serve /models/DeepSeek-V4-Flash \
    --served-model-name {served_model_name} \
    --host {host} \
    --port {port} \
    --trust-remote-code \
    --tensor-parallel-size {tensor_parallel} \
    --pipeline-parallel-size {pipeline_parallel} \
    --enable-expert-parallel \
    --kv-cache-dtype fp8 \
    --block-size {block_size} \
    --enable-prefix-caching \
    --max-model-len {max_model_len} \
    --max-num-seqs {max_num_seqs} \
    --max-num-batched-tokens {max_num_batched_tokens} \
    --gpu-memory-utilization {gpu_memory_utilization} \
    --distributed-executor-backend mp \
    --no-enable-flashinfer-autotune \
    --compilation-config '{{"cudagraph_mode":"FULL_AND_PIECEWISE","custom_ops":["all"]}}' \
    --speculative-config '{{"method":"mtp","num_speculative_tokens":2}}' \
    --tokenizer-mode deepseek_v4 \
    --tool-call-parser deepseek_v4 \
    --enable-auto-tool-choice \
    --reasoning-parser deepseek_v4 \
    --reasoning-config '{{"reasoning_parser":"deepseek_v4","reasoning_start_str":"<think>","reasoning_end_str":"</think>"}}' \
    --default-chat-template-kwargs '{{"thinking":true}}' \
    --load-format safetensor
```

> `model:` 필드는 의도적으로 생략한다. HuggingFace 자동 다운로드를 차단하고, 모델은 `VLLM_SPARK_EXTRA_DOCKER_ARGS`로 호스트 경로를 컨테이너 `/models`에 bind-mount하여 로드한다.

### (참고용) 컨테이너 내부 최종 `vllm serve` 명령

```bash
vllm serve /models/DeepSeek-V4-Flash \
  --served-model-name deepseek-v4-flash \
  --host 0.0.0.0 --port 8000 \
  --trust-remote-code \
  --tensor-parallel-size 2 --pipeline-parallel-size 1 \
  --enable-expert-parallel \
  --kv-cache-dtype fp8 --block-size 256 --enable-prefix-caching \
  --max-model-len 200000 \
  --max-num-seqs 4 --max-num-batched-tokens 16384 \
  --gpu-memory-utilization 0.8 \
  --distributed-executor-backend mp \
  --no-enable-flashinfer-autotune \
  --compilation-config '{"cudagraph_mode":"FULL_AND_PIECEWISE","custom_ops":["all"]}' \
  --speculative-config '{"method":"mtp","num_speculative_tokens":2}' \
  --tokenizer-mode deepseek_v4 \
  --tool-call-parser deepseek_v4 --enable-auto-tool-choice \
  --reasoning-parser deepseek_v4 \
  --reasoning-config '{"reasoning_parser":"deepseek_v4","reasoning_start_str":"<think>","reasoning_end_str":"</think>"}' \
  --default-chat-template-kwargs '{"thinking":true}' \
  --load-format safetensors
```

> `--nnodes`, `--node-rank`, `--master-addr`, `--master-port`는 `launch-cluster.sh`가 no-Ray 모드에서 노드별 자동 부착한다(head=rank 0, worker=rank 1 + `--headless`). recipe에는 포함하지 않는다.

### 핵심 옵션 설명

| 옵션 | 값 | 설명 |
|---|---|---|
| `--tensor-parallel-size` | 2 | 2노드 텐서 병렬, 노드당 ~75 GiB |
| `--enable-expert-parallel` | — | MoE 전문가 병렬 (노드당 128/256 전문가) |
| `--kv-cache-dtype` | fp8 | KV 캐시 fp8 양자화 — 메모리 절감 |
| `--block-size` | 256 | KV 캐시 블록 크기 |
| `--enable-prefix-caching` | — | 반복/공유 프롬프트 프리필 절감 |
| `--max-model-len` | 200000 | 최대 컨텍스트 200K |
| `--speculative-config` | mtp, 2토큰 | MTP 투기적 디코딩 — 디코드 +60% ↑ (A/B 검증) |
| `--compilation-config` | FULL_AND_PIECEWISE | CUDA 그래프 컴파일 모드 |
| `--distributed-executor-backend` | mp | no-Ray 모드 필수 (Ray 사용 시 GB10 토폴로지 오감지) |
| `--gpu-memory-utilization` | 0.8 | GPU 메모리 사용률 80% |
| `CUDA_CACHE_MAXSIZE` | 4GB | CUDA JIT 캐시 상한 — 세션 중 커널 캐시 축출 방지 |

---

## 6. 이미지 빌드 및 노드2 배포

노드1에서 아래 명령 1개로 이미지 빌드와 노드2 자동 복사가 완료된다.

```bash
cd /home/myspark/spark-vllm-docker
./run-recipe.sh deepseek-v4-flash --build-only
```

| 항목 | 값 |
|---|---|
| 이미지 태그 | `vllm-node-dsv4:latest` |
| 빌드 내용 | `jasl/vllm` 포크 commit `b1c97ff068b23858a4759394f8d6c858d822c957` 소스 빌드 |
| 이미지 크기 | 약 19.5 GB |
| 총 빌드 시간 | 약 28분 (vLLM 소스 빌드 ~23분 + 노드2 복사) |
| 노드2 배포 | `COPY_HOSTS` 설정으로 자동 (`docker save | ssh docker load`) |
| wheel 캐시 | `/home/myspark/spark-vllm-docker/wheels/` — 재빌드 시 재활용 |

빌드 완료 후 양 노드에서 동일한 Image ID를 확인한다.

```bash
# 노드1에서 양 노드 이미지 확인
docker images vllm-node-dsv4
ssh 192.168.200.13 docker images vllm-node-dsv4
```

> ⚠️ **재빌드 시 주의** — 기존 이미지가 있으면 빌드를 스킵한다. 재빌드가 필요하면 `--force-build`를 추가할 것. 새 빌드는 torch.compile 캐시 키가 바뀌어 첫 기동 시 전체 재컴파일 → host RAM 스파이크가 발생한다. 첫 기동만 `gpu_memory_utilization`을 낮추거나 스왑 여유를 확보하고 진행할 것.

> 💡 **백업 태그 생성 권장** — 재빌드 전 반드시 현재 이미지를 백업해둘 것 (양 노드 모두):
> ```bash
> docker tag vllm-node-dsv4:latest vllm-node-dsv4:b1c97ff-backup
> ```

---

## 7. 모델 배치

모델(약 149 GB, safetensors 46샤드)은 **양 노드 모두** `/home/myspark/models/DeepSeek-V4-Flash`에 있어야 한다. 한쪽에만 받았다면 클러스터망을 통해 복제한다.

```bash
# 노드2 디렉터리 생성
ssh 192.168.200.13 'mkdir -p /home/myspark/models/DeepSeek-V4-Flash'

# 노드1 → 노드2 병렬 rsync (암호화를 다중 코어로 분산)
cd /home/myspark/models/DeepSeek-V4-Flash
ls | xargs -P 12 -I{} rsync -a --inplace -e 'ssh -c aes128-gcm@openssh.com' \
  {} 192.168.200.13:/home/myspark/models/DeepSeek-V4-Flash/

# 숨김 파일까지 보완
rsync -a --inplace 192.168.200.13:/home/myspark/models/DeepSeek-V4-Flash/ \
  /home/myspark/models/DeepSeek-V4-Flash/
```

> ⚠️ recipe에 `HF_HUB_OFFLINE=1`, `TRANSFORMERS_OFFLINE=1`이 설정되어 있어 모델은 반드시 수동으로 배치해야 한다. vLLM 기동 시 모델 경로는 컨테이너 내부 `/models/DeepSeek-V4-Flash`로 매핑된다.

---

## 8. vLLM 구동 · 종료 · 상태 확인

> **모든 명령은 노드1에서만 실행한다.** 노드2는 노드1이 SSH로 자동 제어한다. 노드2에서 직접 docker stop/start를 하지 말 것.

### 시작

```bash
cd /home/myspark/spark-vllm-docker
VLLM_SPARK_EXTRA_DOCKER_ARGS="-v /home/myspark/models:/models" \
  ./run-recipe.sh deepseek-v4-flash --no-ray --daemon
```

| 옵션 | 설명 |
|---|---|
| `VLLM_SPARK_EXTRA_DOCKER_ARGS` | 호스트 모델 경로를 컨테이너에 마운트 — **필수** |
| `--no-ray` | GB10은 노드당 GPU 1개라 Ray 사용 시 토폴로지 오감지 — **필수** |
| `--daemon` | 백그라운드 기동. 생략 시 포그라운드 로그 스트리밍 |

콜드 스타트(첫 구동 또는 캐시 없을 때): 약 **10분** (149 GB 로드 + torch.compile / TileLang / CUDA 그래프 워밍업). 캐시 warm 시 약 5–6분.

### 종료

```bash
cd /home/myspark/spark-vllm-docker
./launch-cluster.sh -t vllm-node-dsv4 stop
```

### 상태 확인 / 로그

```bash
# 양 노드 컨테이너 상태
./launch-cluster.sh -t vllm-node-dsv4 status

# 노드1 로그 (API + rank 0)
docker logs -f vllm_deepseek_v4_flash

# 노드2 워커 로그
ssh 192.168.200.13 docker logs -f vllm_deepseek_v4_flash
```

### 재시작

```bash
./launch-cluster.sh -t vllm-node-dsv4 stop
VLLM_SPARK_EXTRA_DOCKER_ARGS="-v /home/myspark/models:/models" \
  ./run-recipe.sh deepseek-v4-flash --no-ray --daemon
```

### 롤백 (백업 이미지가 있을 때 — 재빌드 없이 약 6분)

```bash
# 양 노드에서 실행
docker tag vllm-node-dsv4:b1c97ff-backup vllm-node-dsv4:latest

# 노드1에서 재시작
./launch-cluster.sh -t vllm-node-dsv4 stop
VLLM_SPARK_EXTRA_DOCKER_ARGS="-v /home/myspark/models:/models" \
  ./run-recipe.sh deepseek-v4-flash --no-ray --daemon
```

---

## 9. 동작 검증

```bash
# 모델 목록 확인
curl http://192.168.1.12:8000/v1/models

# 채팅 추론 테스트
curl http://192.168.1.12:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "deepseek-v4-flash",
    "messages": [{"role": "user", "content": "안녕하세요"}]
  }'
```

응답의 `reasoning` 필드에 `<think>` 추론 과정이 분리 추출된다.  
API 엔드포인트: `http://192.168.1.12:8000` · 모델명: `deepseek-v4-flash`

### 성능 측정 재현

```bash
ssh -i C:/Users/ine07/.ssh/id_rsa myspark@192.168.1.12 \
  "/home/myspark/tools/llama-benchy/bin/llama-benchy \
     --base-url http://localhost:8000/v1 \
     --model deepseek-v4-flash \
     --tokenizer /home/myspark/models/DeepSeek-V4-Flash \
     --pp 2048 --tg 128 --depth 0 --runs 3 --no-cache"
```

> `--tokenizer`에 모델 경로를 지정해야 t/s가 정확하다 (미지정 시 GPT-2 토크나이저로 폴백).

---

## 10. 성능 측정 결과

측정 도구: `llama-benchy 0.3.7` · 단일 동시성 · depth 0 · 3회 평균 · 측정일 2026-05-22

### MTP(투기적 디코딩) A/B 비교

| 구성 | pp2048 (프리필) | tg128 (디코드) | 평균 수락 | 비고 |
|---|---|---|---|---|
| MTP off (비투기) | 512.1 ± 56.4 | 16.9 ± 0.4 | — | 비투기 한계 |
| `mtp`, 1토큰 | 497.0 ± 50.5 | 25.5 ± 0.3 | 1.8 / 2 | |
| `deepseek_mtp`, 2토큰 | 511.2 ± 36.4 | 27.8 ± 1.7 | 2.2 / 3 | deprecated 별칭 |
| **`mtp`, 2토큰 (현 설정)** | **526.8 ± 17.2** | **26.9 ± 1.2** | **2.3 / 3** | ★ 채택 |
| `mtp`, 2토큰 / **OS 재부팅 후** | **1103.7 ± 4.7** | **38.5 ± 0.6** | — | 동일 빌드, 재부팅만으로 약 2배 |

> ⚠️ OS 재부팅 후 행을 제외한 모든 수치는 장시간 가동으로 열화된 상태에서 측정된 것이다. 절대값은 낮게 잡혀 있으나 레버 간 상대 비교(MTP on/off 등)는 여전히 유효하다.

### 깊이(depth)별 성능 및 포럼 비교

컨테이너 가동 약 43분 시점 측정.

| 테스트 | 본 구성 (tok/s) | 포럼 참고치 (tok/s) | TTFT (본 구성) |
|---|---|---|---|
| pp2048 @d0 | 1009.1 ± 128.1 | 850.0 ± 23.8 | 2.07 s |
| tg128 @d0 | 34.96 ± 2.46 | 37.1 ± 0.8 | — |
| pp2048 @d4096 | 881.3 ± 34.3 | 1070.5 ± 65.1 | 6.99 s |
| tg128 @d4096 | 37.62 ± 0.18 | 34.6 ± 0.9 | — |
| pp2048 @d8192 | 646.7 ± 81.2 | 1122.8 ± 19.3 | 16.08 s |
| tg128 @d8192 | 35.65 ± 2.60 | 34.7 ± 4.0 | — |

디코드(tg)는 전 깊이에서 포럼과 대등(~35–38 tok/s). 프리필(pp)은 깊이가 깊어질수록 본 구성이 하락하는 것이 현 구성의 정량적 약점이다(d8192에서 TTFT 약 2배 느림).

### 기타 측정치

| 항목 | 값 |
|---|---|
| 동시 2요청 집계 디코드 | ~41–43 tok/s |
| GPU SM 클럭 (부하 중, 재부팅 후) | ~2476 MHz |
| GPU SM 클럭 (부하 중, 재부팅 전) | ~2405 MHz |
| 커뮤니티 단일 디코드 참고치 | ~25(bjk110) – 37(serapis) tok/s |

### OS 재부팅 효과 (2026-05-22)

> **핵심 발견**: 재부팅 전 측정치(디코드 ~27, 프리필 ~510)는 "2노드 RoCE 구조적 한계"가 아니라 **장시간 OS 가동으로 누적된 열화 상태**였다. 양 노드 OS 재부팅 후 동일 빌드·동일 설정에서 디코드 **38.5** / 프리필 **1,104**로 약 2배 회복된다.

| | pp2048 | tg128 |
|---|---|---|
| 재부팅 전 (당일 아침 베이스라인) | 530.8 ± 5.0 | 26.7 ± 0.7 |
| **재부팅 후** | **1103.7 ± 4.7** | **38.5 ± 0.6** |

기전 미확정 — 통합 메모리 단편화, 페이지 배치 악화, GPU/드라이버 상태 열화 등으로 추정. 장시간 운영 시 주기적 재부팅이 성능 유지에 효과적이다.

---

## 11. 운영 팁 및 주의사항

### ⚠️ 노드2 직접 제어 금지
노드2에서 직접 `docker stop`을 실행하면 한쪽만 죽어 반쪽 클러스터 상태가 된다. 반드시 노드1에서 `./launch-cluster.sh -t vllm-node-dsv4 stop`으로 종료할 것.

### ⚠️ MTP는 반드시 활성화
MTP off 시 디코드가 16.9 tok/s로 추락한다 (+60% 이상 차이). `method: mtp`, `num_speculative_tokens: 2`가 현 구성 최적치다.

### ⚠️ `No available shared memory broadcast block` 경고
추론 중 60초 이상 freeze 후 이 경고가 나오면 `spark_patch3.py` 미적용으로 JIT 캐시가 소실된 것이다. 패치를 적용하고 캐시를 호스트로 복사하면 해소된다. 패치 후에도 처음 보는 shape는 1회 컴파일이 필요하지만 이후 누적 재사용된다.

### ⚠️ GPU 클럭 부스트 여지 없음
GB10의 설계 동작 클럭은 2418 MHz이며, 부하 중 실측 ~2405–2476 MHz로 이미 그 지점에서 동작한다. `nvidia-smi`의 Max Clocks 3003 MHz는 레지스터 이론값으로 도달 불가하며, `power.limit`도 N/A다. 클럭 튜닝은 레버에서 제외한다.

### ⚠️ 장시간 가동 시 성능 열화 모니터링
node1에 2시간 간격 cron으로 성능 측정 로그가 쌓인다 (`/home/myspark/tools/perf-decay.log`). 성능이 기준치 이하로 하락하면 OS 재부팅으로 회복된다.

### ⚠️ 재빌드 시 OOM 주의
새 빌드는 torch.compile 캐시 키가 바뀌어 첫 기동 시 전체 재컴파일 → host RAM 스파이크. 통합 메모리 여유가 빠듯하므로 첫 기동만 `gpu_memory_utilization`을 0.7 이하로 낮추거나 스왑 여유를 확보하고 진행할 것. (node1 64G, node2 32G 스왑 증설 권장)

### 핵심 경로 참조

| 위치 | 경로 |
|---|---|
| 오케스트레이션 repo (노드1) | `/home/myspark/spark-vllm-docker/` |
| 배포 recipe (노드1) | `/home/myspark/spark-vllm-docker/recipes/deepseek-v4-flash.yaml` |
| 클러스터 .env (노드1) | `/home/myspark/spark-vllm-docker/.env` |
| 모델 (양 노드) | `/home/myspark/models/DeepSeek-V4-Flash` |
| 측정 도구 (노드1) | `/home/myspark/tools/llama-benchy/bin/llama-benchy` |
| 성능 열화 로그 (노드1) | `/home/myspark/tools/perf-decay.log` (cron 2시간) |
| 열화 측정 스크립트 | `/home/myspark/tools/perf-decay-log.sh` |
| recipe 마스터 사본 (로컬) | `C:\Users\ine07\.openharness\deepseek-v4-flash.yaml` |
| 패치 스크립트 (로컬) | `.\patch\spark_patch{,2,3}.py` |

---
## 12. 부록
### 1. Diff 정보
#### Dockerfile
```diff
diff --git a/Dockerfile b/Dockerfile
index 85a7d03..a1c1503 100644
--- a/Dockerfile
+++ b/Dockerfile
@@ -183,6 +183,8 @@ WORKDIR $VLLM_BASE_DIR
 # --- VLLM SOURCE CACHE BUSTER ---
 ARG CACHEBUST_VLLM=1

+# vLLM repository URL (upstream by default; overridable via --vllm-repo)
+ARG VLLM_REPO=https://github.com/vllm-project/vllm.git
 # Git reference (branch, tag, or SHA) to checkout
 ARG VLLM_REF=main

@@ -191,7 +193,7 @@ RUN --mount=type=cache,id=repo-cache,target=/repo-cache \
     cd /repo-cache && \
     if [ ! -d "vllm" ]; then \
         echo "Cache miss: Cloning vLLM from scratch..." && \
-        git clone --recursive https://github.com/vllm-project/vllm.git; \
+        git clone --recursive ${VLLM_REPO} vllm; \
         if [ "$VLLM_REF" != "main" ]; then \
             cd vllm && \
             git checkout ${VLLM_REF}; \
@@ -199,9 +201,11 @@ RUN --mount=type=cache,id=repo-cache,target=/repo-cache \
     else \
         echo "Cache hit: Fetching updates..." && \
         cd vllm && \
+        git remote set-url origin ${VLLM_REPO} && \
         git fetch origin && \
         git fetch origin --tags --force && \
-        (git checkout --detach origin/${VLLM_REF} 2>/dev/null || git checkout ${VLLM_REF}) && \
+        git fetch origin "${VLLM_REF}" && \
+        git checkout --detach FETCH_HEAD && \
         git submodule update --init --recursive && \
         git clean -fdx && \
         git gc --auto; \
```

#### build-and-copy.sh
```diff
diff --git a/build-and-copy.sh b/build-and-copy.sh
index 3526dbb..c5d8264 100755
--- a/build-and-copy.sh
+++ b/build-and-copy.sh
@@ -15,6 +15,8 @@ SSH_USER="$USER"
 NO_BUILD=false
 VLLM_REF="main"
 VLLM_REF_SET=false
+VLLM_REPO="https://github.com/vllm-project/vllm.git"
+VLLM_REPO_SET=false
 FLASHINFER_REF="main"
 FLASHINFER_REF_SET=false
 TMP_IMAGE=""
@@ -303,6 +305,7 @@ while [[ "$#" -gt 0 ]]; do
         --rebuild-flashinfer) REBUILD_FLASHINFER=true ;;
         --rebuild-vllm) REBUILD_VLLM=true ;;
         --vllm-ref) VLLM_REF="$2"; VLLM_REF_SET=true; shift ;;
+        --vllm-repo) VLLM_REPO="$2"; VLLM_REPO_SET=true; shift ;;
         --flashinfer-ref) FLASHINFER_REF="$2"; FLASHINFER_REF_SET=true; shift ;;
         -c|--copy-to|--copy-to-host|--copy-to-hosts)
             COPY_TO_FLAG=true
@@ -569,7 +572,7 @@ if [ "$NO_BUILD" = false ]; then
         # ----------------------------------------------------------
         # Phase 2: vLLM wheels
         # ----------------------------------------------------------
-        if [ "$VLLM_REF_SET" = true ] || [ -n "$VLLM_PRS" ]; then
+        if [ "$VLLM_REF_SET" = true ] || [ "$VLLM_REPO_SET" = true ] || [ -n "$VLLM_PRS" ]; then
             REBUILD_VLLM=true
         fi

@@ -606,7 +609,8 @@ if [ "$NO_BUILD" = false ]; then
                 "--target" "vllm-export"
                 "--output" "type=local,dest=./wheels"
                 "${COMMON_BUILD_FLAGS[@]}"
-                "--build-arg" "VLLM_REF=$VLLM_REF")
+                "--build-arg" "VLLM_REF=$VLLM_REF"
+                "--build-arg" "VLLM_REPO=$VLLM_REPO")

             if [ "$REBUILD_VLLM" = true ]; then
                 VLLM_CMD+=("--build-arg" "CACHEBUST_VLLM=$(date +%s)")
```

#### launch-cluster.sh
```diff
diff --git a/launch-cluster.sh b/launch-cluster.sh
index 6a8021b..79057de 100755
--- a/launch-cluster.sh
+++ b/launch-cluster.sh
@@ -342,6 +342,14 @@ if [[ "$MOUNT_CACHE_DIRS" == "true" ]]; then
     # Triton Cache
     DOCKER_ARGS="$DOCKER_ARGS -v $HOME/.triton:/root/.triton"
     CACHE_DIRS_TO_CREATE+=("$HOME/.triton")
+
+    # TileLang Cache (DeepSeek V4 sparse-MLA / fused kernels)
+    DOCKER_ARGS="$DOCKER_ARGS -v $HOME/.tilelang:/root/.tilelang"
+    CACHE_DIRS_TO_CREATE+=("$HOME/.tilelang")
+
+    # CUDA NVRTC/PTX JIT compute cache (DeepGEMM via DG_JIT_USE_NVRTC)
+    DOCKER_ARGS="$DOCKER_ARGS -v $HOME/.nv:/root/.nv"
+    CACHE_DIRS_TO_CREATE+=("$HOME/.nv")
 fi

 # Resolve launch script path if specified
```


### 2. 추가 파일

#### spark_patch.py
```sh
#!/usr/bin/env python3
"""Patch eugr/spark-vllm-docker to support a custom vLLM repo via --vllm-repo."""
import sys

REPO = "/home/myspark/spark-vllm-docker"


def patch_file(path, edits):
    with open(path) as f:
        content = f.read()
    orig = content
    for old, new, desc in edits:
        if new in content and old not in content:
            print("  [skip] already applied: " + desc)
            continue
        n = content.count(old)
        if n != 1:
            print("  [FAIL] " + desc + ": expected 1 match, found " + str(n))
            sys.exit(1)
        content = content.replace(old, new)
        print("  [ok] " + desc)
    if content != orig:
        with open(path, "w") as f:
            f.write(content)
        print("  -> wrote " + path)
    else:
        print("  -> no change " + path)


print("== Dockerfile ==")
patch_file(REPO + "/Dockerfile", [
    (
        "# Git reference (branch, tag, or SHA) to checkout\nARG VLLM_REF=main",
        "# vLLM repository URL (upstream by default; overridable via --vllm-repo)\n"
        "ARG VLLM_REPO=https://github.com/vllm-project/vllm.git\n"
        "# Git reference (branch, tag, or SHA) to checkout\nARG VLLM_REF=main",
        "add ARG VLLM_REPO",
    ),
    (
        "git clone --recursive https://github.com/vllm-project/vllm.git;",
        "git clone --recursive ${VLLM_REPO} vllm;",
        "parametrize vLLM clone URL",
    ),
    (
        '        echo "Cache hit: Fetching updates..." && \\\n'
        "        cd vllm && \\\n"
        "        git fetch origin && \\",
        '        echo "Cache hit: Fetching updates..." && \\\n'
        "        cd vllm && \\\n"
        "        git remote set-url origin ${VLLM_REPO} && \\\n"
        "        git fetch origin && \\",
        "set origin URL on repo-cache hit",
    ),
])

print("== build-and-copy.sh ==")
patch_file(REPO + "/build-and-copy.sh", [
    (
        'VLLM_REF="main"\nVLLM_REF_SET=false',
        'VLLM_REF="main"\nVLLM_REF_SET=false\n'
        'VLLM_REPO="https://github.com/vllm-project/vllm.git"\nVLLM_REPO_SET=false',
        "add VLLM_REPO defaults",
    ),
    (
        '        --vllm-ref) VLLM_REF="$2"; VLLM_REF_SET=true; shift ;;',
        '        --vllm-ref) VLLM_REF="$2"; VLLM_REF_SET=true; shift ;;\n'
        '        --vllm-repo) VLLM_REPO="$2"; VLLM_REPO_SET=true; shift ;;',
        "add --vllm-repo arg parsing",
    ),
    (
        '        if [ "$VLLM_REF_SET" = true ] || [ -n "$VLLM_PRS" ]; then',
        '        if [ "$VLLM_REF_SET" = true ] || [ "$VLLM_REPO_SET" = true ] || [ -n "$VLLM_PRS" ]; then',
        "trigger rebuild on --vllm-repo",
    ),
    (
        '                "--build-arg" "VLLM_REF=$VLLM_REF")',
        '                "--build-arg" "VLLM_REF=$VLLM_REF"\n'
        '                "--build-arg" "VLLM_REPO=$VLLM_REPO")',
        "pass VLLM_REPO build-arg",
    ),
])

print("PATCH COMPLETE")
```

#### spark_patch2.py
```sh
#!/usr/bin/env python3
"""Patch spark-vllm-docker Dockerfile: fetch the exact VLLM_REF SHA explicitly,
so pinned (possibly force-push-orphaned) commits can be checked out."""
import sys

DOCKERFILE = "/home/myspark/spark-vllm-docker/Dockerfile"

OLD = '        (git checkout --detach origin/${VLLM_REF} 2>/dev/null || git checkout ${VLLM_REF}) && \\\n'
NEW = ('        git fetch origin "${VLLM_REF}" && \\\n'
       '        git checkout --detach FETCH_HEAD && \\\n')

with open(DOCKERFILE) as f:
    content = f.read()

if NEW in content and OLD not in content:
    print("[skip] already applied")
    sys.exit(0)

n = content.count(OLD)
if n != 1:
    print(f"[FAIL] expected exactly 1 match, found {n}")
    sys.exit(1)

content = content.replace(OLD, NEW)
with open(DOCKERFILE, "w") as f:
    f.write(content)
print("[ok] vllm-builder cache-hit: explicit 'git fetch origin <REF>' + checkout FETCH_HEAD")
print("PATCH COMPLETE")
```

#### spark_patch3.py
```sh
#!/usr/bin/env python3
"""Patch launch-cluster.sh: also bind-mount TileLang (~/.tilelang) and the CUDA
NVRTC/PTX JIT compute cache (~/.nv) so DeepSeek-V4 JIT kernels persist across
container restarts. Without this, every restart wipes them and workers
re-compile kernels at runtime -> 'No available shared memory broadcast block'."""
import sys

F = "/home/myspark/spark-vllm-docker/launch-cluster.sh"

OLD = (
    '    # Triton Cache\n'
    '    DOCKER_ARGS="$DOCKER_ARGS -v $HOME/.triton:/root/.triton"\n'
    '    CACHE_DIRS_TO_CREATE+=("$HOME/.triton")\n'
    'fi\n'
)
NEW = (
    '    # Triton Cache\n'
    '    DOCKER_ARGS="$DOCKER_ARGS -v $HOME/.triton:/root/.triton"\n'
    '    CACHE_DIRS_TO_CREATE+=("$HOME/.triton")\n'
    '\n'
    '    # TileLang Cache (DeepSeek V4 sparse-MLA / fused kernels)\n'
    '    DOCKER_ARGS="$DOCKER_ARGS -v $HOME/.tilelang:/root/.tilelang"\n'
    '    CACHE_DIRS_TO_CREATE+=("$HOME/.tilelang")\n'
    '\n'
    '    # CUDA NVRTC/PTX JIT compute cache (DeepGEMM via DG_JIT_USE_NVRTC)\n'
    '    DOCKER_ARGS="$DOCKER_ARGS -v $HOME/.nv:/root/.nv"\n'
    '    CACHE_DIRS_TO_CREATE+=("$HOME/.nv")\n'
    'fi\n'
)

with open(F) as fh:
    c = fh.read()

if ".tilelang:/root/.tilelang" in c and ".nv:/root/.nv" in c:
    print("[skip] already applied")
    sys.exit(0)

n = c.count(OLD)
if n != 1:
    print(f"[FAIL] expected exactly 1 match of anchor, found {n}")
    sys.exit(1)

c = c.replace(OLD, NEW)
with open(F, "w") as fh:
    fh.write(c)
print("[ok] added ~/.tilelang and ~/.nv to mounted cache dirs")
print("PATCH COMPLETE")
```


---

*참고 레시피: [tonyd2wild/deepseek-v4-flash-dual-spark-recipe](https://github.com/tonyd2wild/deepseek-v4-flash-dual-spark-recipe) · 오케스트레이션: [eugr/spark-vllm-docker](https://github.com/eugr/spark-vllm-docker)*
