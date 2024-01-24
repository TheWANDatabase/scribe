import "dotenv/config";
import { spawn } from "child_process";
import { commandOptions, createClient } from "redis";
import { Client, episodeMarkers } from "datakit";
import ffmpeg from "fluent-ffmpeg";
import { existsSync, mkdirSync, readFileSync, statSync } from "fs";
import ytdl from "ytdl-core";
import {
	type PutObjectCommandOutput,
	PutObjectCommand,
	S3Client,
} from "@aws-sdk/client-s3";
import { eq } from "drizzle-orm";

const s3 = new S3Client([
	{
		region: "auto",
		endpoint: `https://${process.env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
		credentials: {
			accessKeyId: process.env.CF_ACCESS_KEY_ID,
			secretAccessKey: process.env.CF_SECRET_ACCESS_KEY,
		},
	},
]);

const redis = createClient({
	username: process.env.REDIS_USER,
	password: process.env.REDIS_PASS,
	url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
});

const db = new Client();

interface TranscriptionTask {
	id: string;
	kind: string;
	vod: string;
}

interface RedisTask {
	id: string;
	message: {
		kind: "youtube" | "floatplane";
		id: string;
		vod: string;
	};
}

redis
	.connect()
	.then(async () => {
		await redis.xGroupCreate("vods", "whisper", "0", {
			MKSTREAM: true,
		});

		while (true) {
			try {
				const [{ messages: tasks }]: any = await redis.xReadGroup(
					commandOptions({
						isolated: true,
					}),
					"whisper",
					"scribe",
					[
						// XREAD can read from multiple streams, starting at a
						// different ID for each...
						{
							key: "vods",
							id: ">",
						},
					],
					{
						// Read 1 entry at a time, block for 5 seconds if there are none.
						COUNT: 1,
						BLOCK: 5000,
					},
				);

				console.log(tasks[0]);

				const { id, message } = tasks[0] as RedisTask;
				const { id: episode, kind, vod } = message as TranscriptionTask;

				console.log(message);
				switch (kind) {
					case "youtube":
						await downloadVideo(vod);
						await transcribeAudio(vod);
						await upload(
							`./transcribed/${vod}.txt`,
							`transcripts/${episode}_yt.txt`,
						);
						await upload(
							`./transcribed/${vod}.json`,
							`transcripts/${episode}_yt.json`,
						);
						await upload(
							`./transcribed/${vod}.vtt`,
							`transcripts/${episode}_yt.vtt`,
						);
						await upload(
							`./transcribed/${vod}.srt`,
							`transcripts/${episode}_yt.srt`,
						);
						await db.data
							.update(episodeMarkers)
							.set({ youtubeCaptions: true })
							.where(eq(episodeMarkers.id, episode));
						await redis.xAck("vods", "whisper", id);
						break;
				}

				process.exit();
			} catch (e) {
				console.error(e);
			}
		}
	})
	.catch(() => {});

async function upload(
	file: string,
	name: string,
): Promise<PutObjectCommandOutput> {
	const stat = statSync(file);
	const body = readFileSync(file);

	return await s3.send(
		new PutObjectCommand({
			Key: name,
			Bucket: process.env.CF_BUCKET_NAME ?? "cdn",
			Body: body,
			ContentLength: stat.size ?? 0,
		}),
	);
}

function downloadVideo(id: string): any {
	return new Promise<void>((resolve, reject) => {
		try {
			if (existsSync(`./audio/${id}.mp3`)) {
				resolve();
				return;
			}
			console.log("> Downloading VOD");
			const stream = ytdl(id, {
				quality: "highestaudio",
			});
			mkdirSync("./audio/", { recursive: true });
			ffmpeg(stream)
				.audioBitrate(128)
				.save(`./audio/${id}.mp3`)
				.on("progress", (p) => {
					process.stdout.write(`\r${p.targetSize}kb downloaded`);
				})
				.on("end", () => {
					resolve();
				});
		} catch (e) {
			reject(e);
		}
	});
}

async function transcribeAudio(id: string): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		console.log("> Starting Transcription Process\xb1[35m");
		const child = spawn(
			"whisperx",
			[
				`./audio/${id}.mp3`,
				"--output_format",
				"all",
				"--device",
				"cuda",
				"--compute_type",
				"float32",
				"--output_dir",
				"transcribed",
				"--model",
				"medium.en",
				"--diarize",
				"--highlight_words",
				"True",
				"--hf_token",
				process.env.HF_TOKEN ?? "",
				"--language",
				"en",
				"--print_progress",
				"True",
				"--threads",
				"12",
			],
			{
				stdio: "inherit",
			},
		);

		// child.stdout.on("data", process.stdoudit.write);
		// child.stderr.on("data", process.stderr.write);
		// process.stdin.on("data", child.stdin.write);

		child.on("exit", () => {
			console.log("\x1b[0m");
			resolve();
		});
		child.on("error", (e) => {
			console.log("\x1b[0m");
			reject(e);
		});
	});
}
