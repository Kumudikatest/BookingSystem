let AWS = require('aws-sdk');
const kinesis = new AWS.Kinesis();
let sqs = new AWS.SQS();
let date = require('date-and-time');
const ddb = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS();

exports.handler = (event, context, callback) => {

	sqs.receiveMessage({
		QueueUrl: 'https://sqs.us-east-1.amazonaws.com/318300609668/KTestSQS',
		AttributeNames: ['All'],
		MaxNumberOfMessages: '10',
		VisibilityTimeout: '30',
		WaitTimeSeconds: '20'
	}).promise()
		.then(data => {
			data.Messages.forEach(message => {      // Going through all the fetched messages in this attempt
				console.log("Received message with payload", message.Body);

				let messageBody = JSON.parse(message.Body);

				let bookingDateObj = new Date();
				let startingDateObj = date.parse(messageBody.bookingRequest.startDate, 'YYYY-MM-DD');
				let endingDateObj = date.parse(messageBody.bookingRequest.endDate, 'YYYY-MM-DD');

				let failure = messageBody.bookingReqProcessingState === "Failed";       // Check whether it's a booking failure
				if (failure) {
					let notificationMsg = "Notifying about booking failure for booking reference :" + messageBody.bookingRef;

					sns.publish({
						Message: notificationMsg,
						MessageAttributes: {
							'AWS.SNS.SMS.SMSType': {
								DataType: 'String',
								StringValue: 'Promotional'
							},
							'AWS.SNS.SMS.SenderID': {
								DataType: 'String',
								StringValue: 'BkFailures'
							}
						},
						PhoneNumber: '+940772445224'
					}).promise()
						.then(data => {
							console.log("Successfully sent notification to the operator with response :" + JSON.stringify(data));
						})
						.catch(err => {
							console.log("Error while sending notification SMS", err);
						});
				}

				let gapForBookingStartDate = date.subtract(startingDateObj, bookingDateObj).toDays();
				let gapBetweenBookingDates = date.subtract(endingDateObj, startingDateObj).toDays();

				// Check whether is it a booking anomaly. In this example it's detected as an anomaly if booking start date is
				// 6 months (180 days) away from the current date or booking date range is greater than 20 days
				if (gapBetweenBookingDates > 20 || gapForBookingStartDate > 180) {
					ddb.put({
						TableName: 'BookingInfoAnomalies',
						Item: {
							'ResellerID': messageBody.resellerId,
							'BookingRef': messageBody.bookingRef,
							'BookingState': !failure,
							'StartDate': messageBody.bookingRequest.startDate,
							'EndDate': messageBody.bookingRequest.endDate,
							'Pax': messageBody.bookingRequest.pax,
							'City': messageBody.bookingRequest.city,
							'Grade': messageBody.bookingRequest.grade,
							'InsertTime': insertTimeStr
						}
					}, function (err, data) {
						if (err) {
							//handle error
							console.log("Error while inserting data to DynamoDB due to : ", err);
						} else {
							//your logic goes here
							console.log("Successfully inserted booking ref : " + messageBody.bookingRef +
								" to DynamoDB with response : " + JSON.stringify(data));
						}
					});
					let insertTimeStr = date.format(new Date(), 'YYYY-MM-DD HH:mm:ss');
				}
			});
		})
		.catch(err => {
			console.log("Error while fetching messages from the sqs queue", err);
		});

	callback(null, 'Lambda execution completed');
};