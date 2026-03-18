STACK_NAME=mictlanx-jub-prod
# COMPOSE_FILE=mictlanx-jub.prod.yml
# COMPOSE_FILE=docker-compose.prod.yml
COMPOSE_FILE=docker-compose.yml
ENV_ROUTER=./.env.dev
ENV_RM=./.mictlanxrm.env.dev
# ENV_ROUTER=./.env.prod
# ENV_RM=./.mictlanxrm.env.prod

.PHONY: deploy rm config

deploy:
	@echo "Deploying MictlanX Router to production with stack name: $(STACK_NAME)"
	export $(grep -v '^#' $(ENV_ROUTER) | xargs) && \
	export $(grep -v '^#' $(ENV_RM) | xargs) && \
	docker stack deploy -c $(COMPOSE_FILE) $(STACK_NAME)
config:
	@echo "Configuration for MictlanX Router (from $(ENV_ROUTER)):"
	@grep -v '^#' $(ENV_ROUTER) | sort
	@echo ""
	@echo "Configuration for MictlanX RM (from $(ENV_RM)):"
	@grep -v '^#' $(ENV_RM) | sort
	@echo ""
	@echo "Docker Compose configuration:"
	docker compose -f $(COMPOSE_FILE) config