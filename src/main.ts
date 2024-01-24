import "dotenv/config";
import { spawn } from "child_process";
import { commandOptions, createClient } from "redis";
import { Client } from "datakit";
import ffmpeg from "fluent-ffmpeg";
import { mkdirSync } from "fs";
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

				console.log(message);
				switch (kind) {
					case "youtube":
						// await downloadVideo(vod);
						await transcribeAudio(vod);
						process.exit();
				}
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
	await new Promise((resolve, reject) => {
		console.log("> Starting Transcription Process");
		console.log(
			"whisperx",
			[
				`./audio/${id}.mp3`,
				"--output_format",
				"all",
				"--device",
				"cuda",
				// "--compute_type",
				// "float16",
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
			].join(" "),
		);
		const child = spawn(
			"whisperx",
			[
				`./audio/${id}.mp3`,
				"--output_format",
				"all",
				"--device",
				"cuda",
				// "--compute_type",
				// "float16",
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
