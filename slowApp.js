"use strict";
import fs from "fs";
import pg from "pg";


const clientConfig = {
	connectionString:
		"postgres://candidate:62I8anq3cFq5GYh2u4Lh@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
	ssl: {
		rejectUnauthorized: true,
		ca: fs
			.readFileSync("c:/Users/batay/.postgresql/root.crt")
			.toString()
	}
}

export const connection = new pg.Client(clientConfig);

connection.connect((err) => {
	if (err) throw err;
});

export const queryText = 'insert into public.bataykin (name, data) values ($1, $2);'


	
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

console.time('Sequential 826 inserts to db')
for (const value of values) {
	await connection.query(queryText, value)
}
console.timeEnd('Sequential 826 inserts to db')

connection.end()

console.log('FINISH! Check the table')



	





    