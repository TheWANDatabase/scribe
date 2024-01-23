import "dotenv/config";
import { spawn } from "child_process";
import { commandOptions, createClient } from "redis";
import { Client } from "datakit";
import ffmpeg from "fluent-ffmpeg";
import { mkdirSync, createWriteStream } from "fs";
import ytdl from "ytdl-core";

const redis = createClient({
	username: process.env.REDIS_USER,
	password: process.env.REDIS_PASS,
	url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
});

interface TranscriptionTask {
	id: string;
	kind: string;
	vod: string;
}

redis
	.connect()
	.then(async () => {
		while (true) {
			try {
				const [{ messages: tasks }]: any = await redis.xRead(
					commandOptions({
						isolated: true,
					}),
					[
						// XREAD can read from multiple streams, starting at a
						// different ID for each...
						{
							key: "vods",
							id: "0",
						},
					],
					{
						// Read 1 entry at a time, block for 5 seconds if there are none.
						COUNT: 1,
						BLOCK: 5000,
					},
				);

				const { message }: { message: TranscriptionTask } = tasks[0];
				const { id: episode, kind, vod } = message;

				switch (kind) {
					case "youtube":
						await downloadVideo(vod);
						await transcribeAudio(vod);
				}

				process.exit();
			} catch (e) {
				console.error(e);
			}
		}
	})
	.catch(() => {});

function downloadVideo(id: string): any {
	return new Promise<void>((resolve, reject) => {
		try {
			console.log("> Downloading VOD");
			const stream = ytdl(id, {
				quality: "highestaudio",
			});
			mkdirSync("./audio/", { recursive: true });
			mkdirSync("./video/", { recursive: true });
			const rs = createWriteStream(`./video/${id}.mp4`);
			let bytes = 0;
			stream.pipe(rs);
			stream.on("data", (chunk) => {
				bytes += chunk.length;
				if (bytes % 10 !== 0) {
					process.stdout.write(
						`\r  > Downloaded ${bytes.toLocaleString()} bytes`,
					);
				}
			});
			stream.on("end", () => {
				const start = Date.now();
				console.log("\n> Converting to MP3");
				ffmpeg(`./video/${id}.mp4`)
					.audioBitrate(128)
					.save(`./audio/${id}.mp3`)
					.on("progress", (p: any) => {
						process.stdout.write(
							`\r  > ${p.currentKbps}kbps | ${p.percent.toFixed(2)}% Completed`,
						);
					})
					.on("end", () => {
						console.log(`\ndone, took ${(Date.now() - start) / 1000}s`);
						resolve();
					});
			});
		} catch (e) {
			reject(e);
		}
	});
}

async function transcribeAudio(id: string): Promise<void> {
	await new Promise((resolve, reject) => {
		console.log("> Starting Transcription Process");
		const child = spawn(
			"whisperx",
			[
				`./audio/${id}.mp3`,
				"--output_format",
				"all",
        "--device",
        "cuda",
        "--compute_type",
        "fp16",
				"--output_dir",
				"transcribed",
				"--model",
				"medium",
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

		child.on("exit", resolve);
		child.on("error", reject);
	});
}
