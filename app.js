"use strict";
import fs from "fs";
import pg from "pg";
import * as os from "os";

import {isMainThread, parentPort, Worker, workerData} from "worker_threads";


const clientConfig = {
	connectionString:
		"postgres://candidate:62I8anq3cFq5GYh2u4Lh@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
	ssl: {
		rejectUnauthorized: true,
		ca: fs
			.readFileSync("./root.crt")
			.toString()
	}
}

export const connection = new pg.Client(clientConfig);

connection.connect((err) => {
	if (err) throw err;
});

export const queryText = 'insert into public.bataykin (name, data) values ($1, $2);'


if (isMainThread) {
	
	console.time('Drop and create table')
	connection.query(`
				DROP TABLE IF EXISTS public.bataykin;

        CREATE TABLE IF NOT EXISTS public.bataykin (
        id serial NOT NULL,
        name text NULL,
        data jsonb NULL);
    `)
	console.timeEnd('Drop and create table')
	
	let next = `https://rickandmortyapi.com/api/character`
	let values = []
	
	console.time('Parse and map data values from API')
	while (next != null) {
		await fetch(`${next}`)
			.then((response) => {
				return response.json();
			})
			.then((data) => {
				next = data?.info?.next
				for (let i = 0; i < data.results.length; i++) {
					values.push([data.results[i].name, data.results[i]])
				}
			});
	}
	console.timeEnd('Parse and map data values from API')
	
	console.time('Chunk data for workers')
	const chunks = []
	const chunkSize = values.length / os.cpus().length;
	for (let i = 0; i < values.length; i += chunkSize) {
		const chunk = values.slice(i, i + chunkSize);
		chunks.push(chunk)
	}
	console.timeEnd('Chunk data for workers')
	
	
	let finished = 0
	console.time('Parallel not sequential 826 inserts to db via 16 workers')
	for await (const chunk of chunks) {
		const worker = new Worker('./app.js', {
			workerData: chunk
		});
		worker.on("message", msg => {
			finished++
			if (finished === chunks.length) {
				console.timeEnd('Parallel not sequential 826 inserts to db via 16 workers')
				console.log('For 10x slower, but sequential insert, try slowApp.js')
				connection.end()
				console.log('FINISH! Check the table')
			}
		});
	}
} else {
	for await (const values of workerData) {
		await connection.query(queryText, values)
	}
	parentPort.postMessage('exit')
	parentPort.close()
}




    