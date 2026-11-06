# Agent Collaboration Coordinator

[English](README.md) · [API](API.md) · [최신 검증 결과](RESULTS-NATIVE.ko.md)

Gitea를 공용 작업 기록으로 유지하면서 여러 사람이 Claude Code·Codex·Roundtable을 병렬로 사용하는 조정자야. 조정자와 각 PC의 작업자를 별도 프로세스로 분리해서, 조정자가 죽어도 작업자가 유지되고 다시 연결할 수 있게 만들었어.

현재는 격리 환경에서 검증한 실행 가능한 구현이야. 기존 운영 자동응답 서비스를 교체하거나 열려 있는 개인 앱 대화에 연결한 상태는 아니야.

## 구현한 동작

- 이슈별 병렬 실행과 같은 이슈의 중복 담당 방지
- 댓글 수정·중복·지연 전달의 원문 대조
- 요구 수락, 작업자 전달, 작업자 확인의 구분
- 특정 실행만 중단하고 최신 요구로 재개
- Claude/Codex의 소유한 native 세션을 명시적으로 재사용
- 조정자 강제 종료 후 같은 작업자 프로세스·native 세션·RT 방에 재연결
- 조정자 중단 중 작성된 댓글 재수집과 미완료 명령 기록 복구
- 조정자 중단 중 완료된 결과를 `result_ready`, `reviewed=false`로 저장
- 외부 게시의 결과가 불명확할 때 원문 대조 전 중복 실행 차단
- 이벤트 기반 상태 댓글 갱신과 자동응답 반복 방지

복구된 결과가 곧 검증 완료나 배포 완료는 아니야. 원래 요구 revision과 결과를 같이 보존하고, 검토와 최종 게시를 별도로 처리해.

## 실제로 확인한 범위

| 시험 | 범위 |
|---|---|
| 구성요소 시험 | Windows·Linux에서 SQLite 경합, 실제 프로세스, 인증, 중단·재개, 복구 기록 |
| 실제 CLI 시험 | Claude Code 2.1.306, Codex 0.163.0 바이너리와 가짜 모델 API |
| native·RT·조정자 복구 | 44개 확인. 조정자를 실제로 강제 종료하고 새 프로세스로 재기동 |
| 물리 PC 두 대 | Windows–macOS 간 12개 확인. 가짜 작업자와 SSH 터널 사용 |
| 이전 Gitea 통합 | 실제 Gitea 28.1.0 및 실제 RT 연결 |

native 시험은 외부 통신이 차단된 Docker `--network none`에서 실행했어. 실제 AI 모델 호출은 0회야. 실제 모델의 이해·코딩 품질이나 유료 계정 사용 권한을 검증한 것은 아니야. 물리 PC 시험과 native·RT 시험은 범위를 나눠 기록했어.

## 사용과 재시험

Python 3.12 이상을 사용하며 Python 패키지 설치는 필요 없어.

```powershell
python -m lab.check
docker build -f Dockerfile.lab -t agent-collaboration-coordinator-lab:local .
```

실제 CLI·RT 시험에는 별도로 설치한 Linux 실행 파일을 사용해. 바이너리·사용자 인증정보·실행 데이터는 Git에 포함하지 않아. native 시험, 실제 Gitea 시험, 물리 PC 시험 명령은 [영문 README](README.md)에 있어.

실제 사용 구조는 다음 순서야.

1. 각 PC에서 `python -m coordinator.worker_host --config <비공개 설정>`으로 작업자를 실행해.
2. 작업자 포트는 loopback에 두고 검증된 SSH 터널로 연결해.
3. 중앙 PC에서 `python -m coordinator.server --config <비공개 설정>`으로 조정자를 실행해.
4. 작업자·담당자·엔진을 지정하고 선택한 Gitea 이슈만 등록해.

설정과 HTTP API는 [API.md](API.md)에 있어. 현재 live 인증은 provider API 자격 증명을 명시하는 방식이야. `allow_model_calls=true`와 모델·API 주소·키 환경 변수 이름을 지정해야 실제 추론이 가능해. 개인 Desktop/OAuth 대화를 가져오지는 않아. 이번 검증에서는 live 모드를 실행하지 않았어.

## 남은 경계

조정자 재시작 복구는 구현했어. 작업자 호스트 자체의 종료나 PC 재부팅은 별개야. 자식 프로세스 소유권을 확인하지 못하면 `unknown`으로 보류해. 같은 주소라도 작업자 DB가 바뀌면 다른 ledger로 판단하고 자동 재실행하지 않아.

아직 수신하지 못한 댓글이나 조정자를 우회한 행동은 차단할 수 없어. 자연어 댓글 전체를 자동 해석하거나 Git worktree 생성·PR 병합·배포를 실행하는 기능은 이번 범위 밖이야. 기존 운영 작업의 담당 전환도 자동으로 하지 않아.
