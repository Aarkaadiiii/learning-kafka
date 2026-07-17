package minchakov.arkadii.common.config;

public record KafkaTopicConfigurationProperties(String name, int replicas, int minInsyncReplicas, int partitions) {
}
