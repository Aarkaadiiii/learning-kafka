package minchakov.arkadii.common.event;

public record NotificationSentEvent(String id, Type type, String productId) {
    public enum Type {
        EMAIL,
        SMS,
        PUSH
    }
}
