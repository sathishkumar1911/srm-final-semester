// Add your AWS credentials and region here
AWS.config.update({
    accessKeyId: '<Access Key Id>',
    secretAccessKey: '<Secret Access Key>',
    region: '<region>'
});

const s3 = new AWS.S3();

function uploadFile() {
    const fileInput = document.getElementById('fileInput');
    const file = fileInput.files[0];

    if (!file) {
        document.getElementById('status').innerText = 'Please select a file.';
        return;
    }

    const params = {
        Bucket: 'smart-device-logs', // S3 bucket name
        Key: `${file.name}`, // The path inside the S3 bucket
        Body: file,
        ContentType: file.type
    };

    s3.upload(params, function(err, data) {
        if (err) {
            document.getElementById('status').innerText = 'Error uploading file.';
            console.error(err);
        } else {
            document.getElementById('status').innerText = 'File uploaded successfully.';
            console.log('File uploaded successfully', data);
        }
    });
}
