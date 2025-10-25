# IAM Policies for Crawl Agent Storage System

## 1. Primary Storage Access Policy (Production)

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PrimaryBucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:HeadObject",
                "s3:HeadBucket",
                "s3:PutObjectAcl",
                "s3:GetObjectAcl"
            ],
            "Resource": [
                "arn:aws:s3:::crawlagent.bucket.a",
                "arn:aws:s3:::crawlagent.bucket.a/*"
            ]
        },
        {
            "Sid": "MultipartUploadAccess",
            "Effect": "Allow",
            "Action": [
                "s3:CreateMultipartUpload",
                "s3:CompleteMultipartUpload",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploads",
                "s3:ListParts",
                "s3:UploadPart"
            ],
            "Resource": "arn:aws:s3:::crawlagent.bucket.a/*"
        },
        {
            "Sid": "BucketVersioningAndLifecycle",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketVersioning",
                "s3:PutBucketVersioning",
                "s3:GetBucketLifecycleConfiguration",
                "s3:PutBucketLifecycleConfiguration",
                "s3:DeleteBucketLifecycleConfiguration"
            ],
            "Resource": "arn:aws:s3:::crawlagent.bucket.a"
        },
        {
            "Sid": "ObjectVersionAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObjectVersion",
                "s3:DeleteObjectVersion",
                "s3:ListBucketVersions"
            ],
            "Resource": [
                "arn:aws:s3:::crawlagent.bucket.a",
                "arn:aws:s3:::crawlagent.bucket.a/*"
            ]
        }
    ]
}
```

## 2. Backup and Disaster Recovery Policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "BackupBucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:HeadObject",
                "s3:HeadBucket",
                "s3:CopyObject"
            ],
            "Resource": [
                "arn:aws:s3:::crawlagent-backup-*",
                "arn:aws:s3:::crawlagent-backup-*/*"
            ]
        },
        {
            "Sid": "CrossRegionReplication",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketReplication",
                "s3:PutBucketReplication",
                "s3:DeleteBucketReplication"
            ],
            "Resource": [
                "arn:aws:s3:::crawlagent.bucket.a",
                "arn:aws:s3:::crawlagent-backup-*"
            ]
        },
        {
            "Sid": "ReplicationRoleAccess",
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::*:role/service-role/replication-role"
        }
    ]
}
```

## 3. Read-Only Policy (for monitoring/analytics)

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadOnlyAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:HeadObject",
                "s3:HeadBucket",
                "s3:GetBucketLocation",
                "s3:GetBucketVersioning",
                "s3:GetBucketLifecycleConfiguration"
            ],
            "Resource": [
                "arn:aws:s3:::crawlagent.bucket.a",
                "arn:aws:s3:::crawlagent.bucket.a/*"
            ]
        },
        {
            "Sid": "MetricsAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketMetricsConfiguration",
                "s3:GetBucketInventoryConfiguration"
            ],
            "Resource": "arn:aws:s3:::crawlagent.bucket.a"
        }
    ]
}
```

## 4. User-Specific Access Policy Template

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "UserFolderAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::crawlagent.bucket.a",
                "arn:aws:s3:::crawlagent.bucket.a/users/${aws:userid}/*"
            ],
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "users/${aws:userid}/*"
                    ]
                }
            }
        },
        {
            "Sid": "DenyAccessToOtherUsers",
            "Effect": "Deny",
            "Action": "s3:*",
            "Resource": "arn:aws:s3:::crawlagent.bucket.a/users/*",
            "Condition": {
                "StringNotLike": {
                    "s3:prefix": [
                        "users/${aws:userid}/*"
                    ]
                }
            }
        }
    ]
}
```

## 5. Service Account Policy (for application)

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ApplicationAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:HeadObject",
                "s3:HeadBucket",
                "s3:CreateMultipartUpload",
                "s3:CompleteMultipartUpload",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploads",
                "s3:UploadPart"
            ],
            "Resource": [
                "arn:aws:s3:::crawlagent.bucket.a",
                "arn:aws:s3:::crawlagent.bucket.a/*"
            ]
        },
        {
            "Sid": "PresignedURLGeneration",
            "Effect": "Allow",
            "Action": [
                "s3:GetObjectPresignedUrl",
                "s3:PutObjectPresignedUrl"
            ],
            "Resource": "arn:aws:s3:::crawlagent.bucket.a/*",
            "Condition": {
                "NumericLessThan": {
                    "s3:signatureAge": 3600
                }
            }
        }
    ]
}
```

## Policy Recommendations:

### For Production:
1. **Use Service Account Policy** for your application
2. **Enable MFA** for administrative operations
3. **Implement IP restrictions** for sensitive operations
4. **Use temporary credentials** with STS when possible

### For User Access:
1. **Use User-Specific Access Policy** to isolate user data
2. **Implement folder-based segregation** (`/users/{user_id}/`)
3. **Add time-based access controls** if needed

### For Backup/DR:
1. **Use separate backup buckets** in different regions
2. **Implement Cross-Region Replication** for critical data
3. **Use lifecycle policies** to manage backup retention
4. **Test restore procedures** regularly

### Security Best Practices:
1. **Rotate access keys** every 90 days
2. **Use IAM roles** instead of long-term access keys when possible
3. **Enable CloudTrail** for audit logging
4. **Implement bucket notifications** for monitoring
5. **Use bucket policies** in addition to IAM policies for defense in depth
