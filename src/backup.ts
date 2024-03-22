import { exec } from "child_process";
import { PutObjectCommand, S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { createReadStream, unlink } from "fs";
import { env } from "./env";

const isDebug = () => {
  return env.DEBUG && env.DEBUG === '1';
};

const uploadToS3 = async (file: { name: string, path: string }): Promise<void> => {
  const bucket = env.AWS_S3_BUCKET;
  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION,
  };

  console.log(`Uploading backup to S3 at ${bucket}/${file.name}...`);

  if (env.AWS_S3_ENDPOINT) {
    console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`);

    clientOptions['endpoint'] = env.AWS_S3_ENDPOINT;
  }

  const client = new S3Client(clientOptions);

  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: file.name,
      Body: createReadStream(file.path),
    })
  );
}

const dumpToFile = async (path: string): Promise<void> => {
  console.log(`Creating dump at ${path}...`);

  await new Promise((resolve, reject) => {
    const host = `--host='${env.BACKUP_DATABASE_HOST}'`;
    const port = `--port='${env.BACKUP_DATABASE_PORT}'`;
    const user = `--user='${env.BACKUP_DATABASE_USER}'`;
    const password = `--password='${env.BACKUP_DATABASE_PASSWORD}'`;
    const databasesToExclude = ['mysql', 'sys', 'performance_schema', 'information_schema', 'innodb'].join('|');

    const command = env.BACKUP_DATABASE_NAME
      ? `mysqldump ${host} ${port} ${user} ${password} ${env.BACKUP_DATABASE_NAME} | gzip > ${path}`
      : `mysql ${host} ${port} ${user} ${password} -e "show databases;" | grep -Ev "Database|${databasesToExclude}" | xargs mysqldump ${host} ${port} ${user} ${password} --databases | gzip > ${path}`

    if (isDebug()) {
      console.log(`Debug: SQL command: ${command}`);
    }

    exec(command, (error, _, stderr) => {
      if (error) {
        reject({ error: JSON.stringify(error), stderr });

        if (isDebug()) {
          console.log(`Debug: could not create local dump file. ${error}`);
        }

        return;
      }

      resolve(undefined);
    });
  });
}

const deleteFile = async (path: string): Promise<void> => {
  console.log(`Deleting local dump file at ${path}...`);

  await new Promise((resolve, reject) => {
    unlink(path, (error) => {
      reject({ error: JSON.stringify(error) });

      if (error && isDebug()) {
        console.log(`Debug: could not remove local dump file. ${error}`);
      }

      return;
    });
    resolve(undefined);
  });
}

export const backup = async (): Promise<void> => {
  const timestamp = new Date().toISOString().replace(/[:.]+/g, '-');
  const filename = `backup-${timestamp}.sql.gz`;
  const filepath = `/tmp/${filename}`;

  await dumpToFile(filepath);
  await uploadToS3({ name: filename, path: filepath });
  await deleteFile(filepath);

  console.log("Backup successfully created.");
}
