package transcribe

import java.util.UUID

import com.amazonaws.services.lambda.runtime.events.S3Event
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, PutItemRequest}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CopyObjectRequest, DeleteObjectRequest}
import software.amazon.awssdk.services.transcribe.TranscribeClient
import software.amazon.awssdk.services.transcribe.model.{Media, Settings, StartTranscriptionJobRequest}

import scala.jdk.CollectionConverters._

/**
 * Class used to process transcribe requests.
 */
class App extends RequestHandler[S3Event, String] {
  /** DynamoDB client. */
  val dynamoDbClient: DynamoDbClient = DynamoDbClient.builder().build()
  /** Code for the language to use in Transcribe. */
  val languageCode: String = sys.env.getOrElse("LANGUAGE_CODE", "es_US")
  /** Client for S3. */
  val s3Client: S3Client = S3Client.builder().build()
  /** DynamoDB table name. */
  val tableName: String = sys.env.getOrElse("TABLE_NAME", "transcribe-sentiment-poc-table")
  /** Client for Amazon Transcribe. */
  val transcribeClient: TranscribeClient = TranscribeClient.builder().build()

  /**
   * Process the S3 event, moves the audio file to a new location and sends it to Amazon Transcribe.
   *
   * @param input S3Event with the created file key.
   * @param context of the execution.
   * @return Outcome of the execution.
   */
  override def handleRequest(input: S3Event, context: Context): String = {
    val logger = context.getLogger;
    for (record <- input.getRecords.asScala) {
      val oldKey = record.getS3.getObject.getKey
      val ext = oldKey.substring(oldKey.lastIndexOf('.'))
      val jobId = UUID.randomUUID().toString
      val newKey = s"processed_audio/${jobId}${ext}"
      logger.log(oldKey)
      logger.log(newKey)
      val bucketName = record.getS3.getBucket.getName
      val copyObjectRequest = CopyObjectRequest.builder()
        .destinationBucket(bucketName)
        .destinationKey(newKey)
        .copySource(s"${bucketName}/${oldKey}")
        .destinationBucket(bucketName)
        .build()
      val copyObjectResponse = s3Client.copyObject(copyObjectRequest)
      val deleteObjectRequest = DeleteObjectRequest.builder()
        .bucket(bucketName)
        .key(oldKey)
        .build()
      val deleteObjectResponse = s3Client.deleteObject(deleteObjectRequest)
      logger.log(copyObjectResponse.copyObjectResult().toString)
      val media = Media.builder()
        .mediaFileUri(s"s3://${bucketName}/${newKey}")
        .build()
      val settings = Settings.builder()
        .maxSpeakerLabels(4)
        .showSpeakerLabels(true)
        .build()
      val startTranscriptionJobRequest = StartTranscriptionJobRequest.builder()
        .languageCode(languageCode)
        .media(media)
        .outputBucketName(bucketName)
        .settings(settings)
        .transcriptionJobName(jobId)
        .build()
      val startTranscriptionJobResponse = transcribeClient.startTranscriptionJob(startTranscriptionJobRequest)
      val item = Map.newBuilder[String, AttributeValue]
      item += "id" -> AttributeValue.builder().s(jobId).build()
      item += "date" -> AttributeValue.builder()
        .n(record.getEventTime.toDate.getTime.toString)
        .build()
      item += "s3_key" -> AttributeValue.builder().s(newKey).build()
      item += "started" -> AttributeValue.builder()
        .n(startTranscriptionJobResponse.transcriptionJob().creationTime().toEpochMilli.toString)
        .build()
      val putItemRequest = PutItemRequest.builder()
        .tableName(tableName)
        .item(item.result().asJava)
        .build()
      val response = dynamoDbClient.putItem(putItemRequest)
      logger.log(response.toString)
    }
    "Ok"
  }
}
