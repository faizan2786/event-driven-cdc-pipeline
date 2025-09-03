package kafkautils

import (
	"context"
	"fmt"
	"time"

	"github.com/faizan2786/event-driven-cdc-pipeline/cdc-pipeline/internal/logger"
	"github.com/segmentio/kafka-go"
)

// check the status of the group
// keep checking with a backoff until the group status is "empty" or "stable"
func WaitForGroupReady(brokers []string, groupId string, maxAttempts int, backOffTimeout int) error {

	// allow some time to reflect the consumer group metadata in the cluster
	time.Sleep(time.Duration(backOffTimeout) * time.Second)

	// Now, query for the group status...

	client := &kafka.Client{
		Addr:    kafka.TCP(brokers...),
		Timeout: 10 * time.Second,
	}

	for i := 0; i < maxAttempts; i++ {
		// get the consumer group info from the kafka broker
		res, err := client.DescribeGroups(
			context.Background(),
			&kafka.DescribeGroupsRequest{
				GroupIDs: []string{groupId},
			})

		if err != nil {
			return fmt.Errorf("error while getting group info from Kafka: %w", err)
		}

		// Check if there is any group specific error
		groupErr := res.Groups[0].Error
		if groupErr != nil {
			return fmt.Errorf("error while fetching group info for group '%s': %w", groupId, err)
		}

		// Check the group state and retry if it is not "empty" or "stable"
		state := res.Groups[0].GroupState
		logger.DebugLogger.Printf("'%s' group state received: %s\n", groupId, state)
		if state == "Empty" || state == "Stable" {
			logger.DebugLogger.Printf("Group '%s' is ready to consume messages\n", groupId)
			return nil
		}
		delay := backOffTimeout << i
		logger.DebugLogger.Printf("[Attempt %d/%d] Group '%s' is not ready, waiting for %d seconds before next retry...\n", i+1, maxAttempts, groupId, delay)
		time.Sleep(time.Duration(delay) * time.Second)
	}

	return fmt.Errorf("group '%s' is not ready even after maximum attempts. aborting operation", groupId)
}
