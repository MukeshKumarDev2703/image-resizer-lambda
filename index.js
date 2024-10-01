const AWS = require('aws-sdk');
const sharp = require('sharp'); // Using sharp to resize images
const sns = new AWS.SNS();
const s3 = new AWS.S3();
const cloudwatch = new AWS.CloudWatch();

const srcBucket = process.env.SRC_BUCKET;
const destBucket = process.env.DEST_BUCKET;
const resizeSnsTopic = process.env.RESIZE_SNS_TOPIC;
const highActivitySns = process.env.HIGH_ACTIVITY_SNS;

// In-memory object to track resize counts
let resizeCount = { total: 0 };
let resizeTimeWindow = { lastReset: Math.floor(Date.now() / 1000) };

// Resize the image using sharp
const resizeImage = async (imageBuffer, width = 100, height = 100) => {
    return await sharp(imageBuffer)
        .resize(width, height)
        .toFormat('png')
        .toBuffer();
};

const publishCustomMetric = async () => {
    await cloudwatch.putMetricData({
        Namespace: 'ImageResizer',
        MetricData: [{
            MetricName: 'ResizeCount',
            Unit: 'Count',
            Value: 1
        }]
    }).promise();
};

const notifyHighActivity = async () => {
    if (resizeCount.total >= 5) {
        await sns.publish({
            TopicArn: highActivitySns,
            Message: `High activity detected: ${resizeCount.total} images resized in the last 10 minutes.`
        }).promise();
        resizeCount.total = 0; // Reset the count after notification
    }
};

exports.handler = async (event) => {
    // Check if Records is present in the event object
    if (!event.Records) {
        console.error('No records found in event:', JSON.stringify(event));
        return;
    }

    const currentTime = Math.floor(Date.now() / 1000);

    for (const record of event.Records) {
        const srcKey = record.s3.object.key;

        // Download the file from the source bucket
        const response = await s3.getObject({ Bucket: srcBucket, Key: srcKey }).promise();
        const fileContent = response.Body;

        // Resize the image
        const resizedImage = await resizeImage(fileContent);

        // Upload the resized image to the destination bucket
        const destKey = `resized-${srcKey}`;
        await s3.putObject({ Bucket: destBucket, Key: destKey, Body: resizedImage }).promise();

        // Publish notification to SNS about resize success
        await sns.publish({
            TopicArn: resizeSnsTopic,
            Message: `Image ${srcKey} resized and uploaded as ${destKey} to bucket ${destBucket}.`
        }).promise();

        // Publish a custom metric for each resized image
        await publishCustomMetric();

        // Update resize count
        if (currentTime - resizeTimeWindow.lastReset < 600) {  // within the last 10 minutes
            resizeCount.total += 1;
        } else {  // Reset count after 10 minutes
            resizeCount.total = 1;
            resizeTimeWindow.lastReset = currentTime;
        }

        // Notify if high activity is detected
        await notifyHighActivity();
    }
};
