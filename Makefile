test.run: test.mysql.run

test.mysql.run:
	docker run --name mysql-proxy-mysql -d \
		-e MYSQL_ROOT_PASSWORD=root \
		-e MYSQL_ROOT_HOST=% \
		-p 3306:3306 \
		mysql/mysql-server:8.0.23

test.clean:
	docker rm -f mysql-proxy-mysql
