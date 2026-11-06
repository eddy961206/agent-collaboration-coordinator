# Native 연결과 다중 PC·조정자 복구 검증

2026-11-06 KST에 격리 환경에서 검증했다. 기존 운영 서비스에는 반영하지 않았다.

## 세 가지 검증 범위

1. **실제 native CLI**: Claude Code 2.1.306, Codex 0.163.0을 새 `--network none` 컨테이너에서 실행했다. 모델 API만 로컬 가짜 서버로 대체했다. 중간 입력, 중단, 같은 native session/thread ID로 최신 요청 재개, 정상 완료를 확인했다.
2. **native·RT·조정자 프로세스**: 실제 CLI 두 종류와 실제 RT를 두 WorkerHost에 연결했다. 조정자를 실제 SIGKILL한 뒤 새 PID로 재기동했다. 같은 직접 실행 PID·native 세션·RT 방을 복구했다. 이후 조정자를 다시 종료한 상태에서 작업을 완료하고, 다음 조정자가 결과를 검토 대기로 수집했다. 44개 확인이 통과했다.
3. **실제 Windows–Mac 연결**: 서로 다른 물리 PC에서 가짜 작업자를 실행했다. SSH 터널 단절·재연결, 새 클라이언트, 변경·중단·재개를 확인했다. 12개 확인이 통과했다. 이 시험에서는 native CLI나 RT를 실행하지 않았다.

실제 모델 호출은 모두 0회다. native 통합에서 가짜 provider가 받은 요청은 12회이며, 이를 실제 추론으로 세지 않는다. 구성요소 시험의 현재 수와 실행 환경은 `reports/unit-windows.json`, `reports/unit-linux.json`에 있다.

## 복구 규칙과 확인 근거

| 조건 | 구현·검증 결과 |
|---|---|
| 조정자만 강제 종료 | PC별 작업자가 계속 실행되고 같은 실행 ID로 재연결 |
| launch 기록 후 원격 전송 전 종료 | 저장한 동일 launch와 prepared handle로 idempotent 전송 |
| worker 시작 후 응답 기록 전 종료 | 원격 ledger의 같은 실행을 찾아 새 프로세스를 만들지 않음 |
| 중단 의도 기록 후 상태 전환 전 종료 | 정확히 지정한 실행의 중단을 복구 |
| 준비된 handle 없이 중단 | mutating RPC 이전임을 확인해 실행 전 취소 |
| 중단 중 요구 변경 | 요구 수락을 보존하고 재개 후 전달 |
| 재연결 후 변경 전달 실패 | working으로 방치하지 않고 recovering 상태로 유지 |
| 조정자 중단 중 결과 완료 | 결과를 result_ready로 저장; 자동 검증·게시하지 않음 |
| 원격 ledger 교체 | hostname/token이 같아도 기존 handle 거절 |
| 외부 게시 응답 유실 | 원문 확인 전 새 operation ID를 통한 우회도 차단 |
| 실패한 실행의 부분 출력 | 결과 텍스트가 있어도 정상 완료로 승격하지 않음 |

Claude의 중간 입력은 먼저 `queued`로 확인될 수 있다. 이 영수증은 입력이 대기열에 들어갔다는 뜻이다. 실제 요청 시작·의미 이해·요구 충족과 구분한다. Codex는 app-server의 thread/turn 및 interrupt 프로토콜을 사용한다.

## 재현과 공개 근거

- `reports/native-smoke-summary.json`: 두 실제 CLI의 세션 재개 확인과 바이너리 해시
- `reports/native-distributed-summary.json`: 44개 관측, 실제 프로그램 버전과 해시
- `reports/physical-pc-summary.json`: 실제 두 PC의 12개 관측과 정리 결과
- `tests/test_recovery.py`: 상태 전환 직전·직후 종료 조건, 원문 재수집, 결과 보존 회귀
- `.runtime/`: 비공개 원문 입력·프로세스 ID·세션 ID·임시 설정. Git 제외

RT는 소스 commit `383e02d6dc2feed92865baeb0443c9bc37deea63`의 기존 격리 빌드를 사용했다. 현재 사용자의 운영 RT나 호스트 CLI 버전을 검증한 것으로 확대하지 않는다. 실제 native 시험과 물리 PC 시험은 별도이며, 모든 OS·모델·PC 조합을 전수 시험한 것이 아니다.

## 미포함

실제 모델 추론·코딩 품질, 기존 개인 Desktop/OAuth 대화 연결, 운영 이슈 담당 전환과 배포는 검증하지 않았다. live native 인증은 명시한 provider API 자격 증명을 사용하는 새 관리 세션 경로다. 작업자 호스트 자체가 종료되면 소유권을 확인할 수 없는 작업은 unknown으로 남는다. 이번 복구 구현은 조정자 프로세스의 종료·재기동을 대상으로 한다.

참고한 1차 문서: [Codex App Server](https://learn.chatgpt.com/docs/app-server), [Codex provider configuration](https://learn.chatgpt.com/docs/config-file/config-advanced), [Claude programmatic execution](https://code.claude.com/docs/en/headless), [Claude gateway configuration](https://code.claude.com/docs/en/llm-gateway). 실제 검증 결과는 위 로컬 보고서와 고정 버전을 기준으로 한다.
