build:
	docker-compose -f docker/docker-compose.yml build

up:
	docker-compose -f docker/docker-compose.yml up

upc:
	docker-compose -f docker/docker-compose-console.yml up

downc:
	docker-compose -f docker/docker-compose-console.yml down

down:
	docker-compose -f docker/docker-compose.yml down

run:
	docker-compose -f docker/docker-compose.yml run app python manage.py $(c)

shell:
	docker-compose -f docker/docker-compose.yml run app /bin/ash

build-svr:
	docker-compose -f docker/resolution-service.yml build

up-svr:
	docker-compose -f docker/resolution-service.yml up

down-svr:
	docker-compose -f docker/resolution-service.yml down
