import "dotenv/config";
import { spawn } from "child_process";
import { commandOptions, createClient } from "redis";
import { Client, episodeMarkers } from "datakit";
import ffmpeg from "fluent-ffmpeg";
import { existsSync, mkdirSync, readFileSync, statSync, unlinkSync } from "fs";
import ytdl from "ytdl-core";
import {
	type PutObjectCommandOutput,
	PutObjectCommand,
	S3Client,
} from "@aws-sdk/client-s3";
import { eq } from "drizzle-orm";

const s3Config: any = {
	region: "auto",
	endpoint: `https://${process.env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
	credentials: {
		accessKeyId: process.env.CF_ACCESS_KEY_ID,
		secretAccessKey: process.env.CF_SECRET_ACCESS_KEY,
	},
};

// eslint-disable-next-line @typescript-eslint/no-unsafe-argument
const s3 = new S3Client(s3Config);

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
		const makeGroup = (await redis.exists("vods")) === 0;

		if (makeGroup) {
			console.log("Configuring xGroups");
			await redis.xGroupCreate("vods", "whisper", "0", {
				MKSTREAM: true,
			});
		}

		console.log("Starting event stream loop");

		while (true) {
			try {
				const response = await redis.xReadGroup(
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
				if (response === null) {
					console.log("Queue empty. Checking again shortly...");
					continue;
				}
				const [{ messages: tasks }] = response;

				const { id, message } = tasks[0] as RedisTask;
				const { id: episode, kind, vod } = message as TranscriptionTask;

				const timings = {
					download: 0,
					transcribe: 0,
					upload: 0,
					job: 0,
				};

				const timers = {
					download: new Date(),
					transcribe: new Date(),
					upload: new Date(),
					job: new Date(),
				};
				switch (kind) {
					case "youtube":
						console.log(
							`Starting transcript job for episode ${episode} (Job Type: ${kind})`,
						);
						timers.download = new Date();
						await downloadVideo(vod);
						timings.download = Date.now() - timers.download.getTime();
						console.clear();
						console.log(
							`Starting transcript job for episode ${episode} (Job Type: ${kind})`,
						);
						console.log(
							`Download - Done (Took ${toHumanTime(timings.download)})`,
						);
						timers.transcribe = new Date();
						await transcribeAudio(vod);
						timings.transcribe = Date.now() - timers.transcribe.getTime();
						console.clear();
						console.log(
							`Starting transcript job for episode ${episode} (Job Type: ${kind})`,
						);
						console.log(
							`> Download - Done (Took ${toHumanTime(timings.download)})`,
						);
						console.log(
							`> Transcribe - Done (Took ${toHumanTime(timings.download)})`,
						);
						timers.upload = new Date();
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
						timings.upload = Date.now() - timers.upload.getTime();
						console.clear();
						console.log(
							`Starting transcript job for episode ${episode} (Job Type: ${kind})`,
						);
						console.log(
							`> Download - Done (Took ${toHumanTime(timings.download)})`,
						);
						console.log(
							`> Transcribe - Done (Took ${toHumanTime(timings.download)})`,
						);
						console.log(
							`> Upload - Done (Took ${toHumanTime(timings.download)})`,
						);
						await db.data
							.update(episodeMarkers)
							.set({ youtubeCaptions: true })
							.where(eq(episodeMarkers.id, episode));
						await redis.xAck("vods", "whisper", id);
						console.clear();
						console.log(
							`Starting transcript job for episode ${episode} (Job Type: ${kind})`,
						);
						console.log(
							`> Download - Done (Took ${toHumanTime(timings.download)})`,
						);
						console.log(
							`> Transcribe - Done (Took ${toHumanTime(timings.transcribe)})`,
						);
						console.log(
							`> Upload - Done (Took ${toHumanTime(timings.upload)})`,
						);
						console.log("> Cleaning up....");
						console.log(`  > ./audio/${vod}.mp3`);
						if (existsSync(`./audio/${vod}.mp3`))
							unlinkSync(`./audio/${vod}.mp3`);
						console.log(`  > ./transcribed/${vod}.json`);
						if (existsSync(`./transcribed/${vod}.json`))
							unlinkSync(`./transcribed/${vod}.json`);
						console.log(`  > ./transcribed/${vod}.vtt`);
						if (existsSync(`./transcribed/${vod}.vtt`))
							unlinkSync(`./transcribed/${vod}.vtt`);
						console.log(`  > ./transcribed/${vod}.srt`);
						if (existsSync(`./transcribed/${vod}.srt`))
							unlinkSync(`./transcribed/${vod}.srt`);
						console.log(`  > ./transcribed/${vod}.txt`);
						if (existsSync(`./transcribed/${vod}.txt`))
							unlinkSync(`./transcribed/${vod}.txt`);
						console.log(`  > ./transcribed/${vod}.tsv`);
						if (existsSync(`./transcribed/${vod}.tsv`))
							unlinkSync(`./transcribed/${vod}.tsv`);
						console.clear();
						timings.job = Date.now() - timers.job.getTime();
						console.log(
							`Starting transcript job for episode ${episode} (Job Type: ${kind})`,
						);
						console.log(
							`> Download - Done (Took ${toHumanTime(timings.download)})`,
						);
						console.log(
							`> Transcribe - Done (Took ${toHumanTime(timings.transcribe)})`,
						);
						console.log(
							`> Upload - Done (Took ${toHumanTime(timings.upload)})`,
						);

						console.log(`> Episode - Done (Took ${toHumanTime(timings.job)})`);
						break;
					case "floatplane":
						await redis.xAck("vods", "whisper", id);
				}

				// process.exit();
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
		console.log("> Starting Transcription Process\x1b[35m");

		if (existsSync(`./transcribed/${id}.vtt`)) {
			resolve();
			return;
		}

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

function toHumanTime(total: number): string {
	total = Math.abs(total / 1000);
	const seconds = Math.floor(total % 60);
	const minutes = Math.floor((total / 60) % 60);
	const hours = Math.floor((total / (60 * 60)) % 24);
	const days = Math.floor(total / (60 * 60 * 24));
	let result = "";

	if (days > 0) result += (days > 9 ? days : "0" + days) + ":";
	if (hours > 0) result += (hours > 9 ? hours : "0" + hours) + ":";

	result += (minutes > 9 ? minutes : "0" + minutes) + ":";
	result += seconds > 9 ? seconds : "0" + seconds;

	return result;
}
