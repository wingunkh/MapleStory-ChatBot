package com.maple.batch;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling // Spring Scheduling 활성화
@RequiredArgsConstructor
@Slf4j
public class JobScheduler {
    private final Job myJob;
    private final JobLauncher jobLauncher;

    @Scheduled(cron = "0 0 3 * * ?") // 매일 새벽 3시
    public void runJob() {
        executeJob();
    }

    @PostConstruct
    // 스프링 빈의 이벤트 라이프사이클
    // 스프링 컨테이너 생성 → 스프링 빈 생성 → 의존관계 주입 → 초기화 콜백 → 사용 → 소멸 전 콜백 → 스프링 종료
    // @PostConstruct : 초기화 콜백 (빈이 생성되고, 빈의 의존관계 주입이 완료된 후 호출)
    // @PreDestroy : 소멸 전 콜백 (빈이 소멸되기 직전에 호출)
    public void init() {
        executeJob();
    }

    public void executeJob() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    // JobParameters는 Job 인스턴스 식별을 위해 고유한 값을 포함해야 함
                    .toJobParameters();

            jobLauncher.run(myJob, jobParameters);
        } catch (Exception e) {
            log.error("Job Execution Error: ", e);
        }
    }
}

    /*
        [Trouble Shooting]
        기존 Line 34의 경우 (jobExecution.getStatus() == BatchStatus.COMPLETED) 조건을 만족할 때 소요 시간을 로깅
        그러나 BatchConfig.java에서 각 Step을 비동기 병렬 처리하도록 변경한 후,
        Job이 성공적으로 수행되었음에도 소요 시간을 측정할 수 없는 문제가 발생
        원인은 각 Step이 비동기 병렬 처리 중이기 때문에, jobLauncher.run() 메서드를 호출한 이후 BatchStatus가 STARTING이였기 때문
        즉, 기존 로깅 방식이 잘못되었음을 알 수 있었고 JobExecutionListener를 통해 로깅하도록 변경함
    */
