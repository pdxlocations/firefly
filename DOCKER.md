# Docker Setup for Firefly

This guide will help you run Firefly using Docker and Docker Compose, based on the excellent setup from the [malla project](https://github.com/zenitraM/malla).

## Quick Start

1. **Clone the repository** (if not already done):
   ```bash
   git clone https://github.com/pdxlocations/firefly.git
   cd firefly
   ```

2. **Set up environment** (optional):
   ```bash
   cp .env.example .env
   # Edit .env file with your preferred settings
   ```

3. **Build and run**:
   ```bash
   docker-compose up --build
   ```
   
   **For optimal UDP multicast on Linux** (optional):
   - Edit `docker-compose.yml`
   - Comment out the `ports:` and `networks:` sections
   - Uncomment `network_mode: host`
   - Run: `docker-compose up --build`

4. **Access the application**:
   - Open your browser to http://localhost:5011
   - The application will be ready to use!

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and customize:

```bash
cp .env.example .env
```

Key variables:
- `FIREFLY_WEB_PORT`: Web interface port (default: 5011)
- `FIREFLY_SECRET_KEY`: Flask secret key (change in production!)
- `FIREFLY_UDP_PORT`: UDP port for Meshtastic communication (default: 4403)
- `FIREFLY_DEBUG`: Enable debug mode (default: false)

### Docker Compose Options

#### Development Mode (Local Build)
```bash
# Uses local Dockerfile (default configuration)
docker-compose up --build
```

#### Production Mode (Pre-built Image)
```bash
# Uncomment the image line and comment build line in docker-compose.yml
# FIREFLY_IMAGE=ghcr.io/pdxlocations/firefly:latest docker-compose up
```

## Data Persistence

The application data (database, profiles) is stored in a Docker volume:
- Volume name: `firefly_data`
- Container path: `/app/data`
- Contains: SQLite database and any generated files

### Backup Data
```bash
# Create backup
docker run --rm -v firefly_data:/data -v $(pwd):/backup alpine tar czf /backup/firefly-backup.tar.gz -C /data .

# Restore backup  
docker run --rm -v firefly_data:/data -v $(pwd):/backup alpine tar xzf /backup/firefly-backup.tar.gz -C /data
```

## Network Configuration

### UDP Communication
Firefly communicates with Meshtastic devices via UDP multicast. This can be challenging in Docker:

#### Docker Networking Options

**Option 1: Bridge Network with Port Mapping (Default)**
```bash
docker-compose up --build
```
- Uses custom bridge network
- Maps UDP port 4403 to host
- Works on all platforms
- May have multicast limitations

**Option 2: Host Network (Linux only)**

Edit `docker-compose.yml` to enable host networking:
1. Comment out the `ports:` and `networks:` sections
2. Uncomment `network_mode: host`
3. Run: `docker-compose up --build`

Benefits:
- Uses host networking directly
- Full multicast support on Linux
- Best for production Meshtastic communication

#### Requirements
1. UDP port 4403 (or your configured port) is accessible
2. Your Meshtastic device is configured to send UDP packets to your Docker host
3. If using bridge network, ensure multicast is properly forwarded
4. Host networking may be required for full multicast support

## Useful Commands

### View Logs
```bash
docker-compose logs -f firefly
```

### Shell Access
```bash
docker-compose exec firefly /bin/bash
```

### Restart Service
```bash
docker-compose restart firefly
```

### Update and Restart
```bash
docker-compose pull  # If using pre-built image
docker-compose up --build -d  # If building locally
```

### Clean Start
```bash
docker-compose down
docker-compose up --build
```

## Troubleshooting

### Container Won't Start
1. Check logs: `docker-compose logs firefly`
2. Verify port availability: `netstat -tulpn | grep :5011`
3. Check disk space: `df -h`

### UDP Communication Issues
**This is the most common issue with Docker deployments.**

#### Recommended Solution for Linux
```bash
# Stop current container
docker-compose down

# Edit docker-compose.yml to enable host networking:
# 1. Comment out 'ports:' and 'networks:' sections
# 2. Uncomment 'network_mode: host'
# 3. Restart
docker-compose up --build
```

#### macOS/Windows Users
```bash
# Docker Desktop has UDP multicast limitations
# Recommend native installation for development:
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
python3 start.py
```

#### Troubleshooting Steps
1. **Check if UDP port is exposed**: `docker ps` should show `0.0.0.0:4403->4403/udp`
2. **Test UDP connectivity**: `nc -u localhost 4403`
3. **Verify firewall settings**: `sudo ufw status` (Ubuntu/Debian)
4. **Check container logs**: `docker-compose logs firefly`
5. **Test multicast capability**: 
   ```bash
   # Test multicast reception
   docker-compose exec firefly nc -u -l 4403
   ```

### Database Issues
1. Database is stored in Docker volume `firefly_data`
2. Reset database: `docker volume rm firefly_data` (WARNING: This deletes all data!)
3. Check database permissions in container

### Performance Issues
1. Increase container resources in Docker Desktop/daemon
2. Check system resources: `docker stats firefly-app`
3. Monitor logs for errors: `docker-compose logs -f firefly`

## Security Considerations

### Production Deployment
1. **Change secret key**: Set `FIREFLY_SECRET_KEY` to a secure random value
2. **Use reverse proxy**: Consider nginx or traefik for HTTPS termination
3. **Firewall**: Restrict access to necessary ports only
4. **Updates**: Keep base image and dependencies updated
5. **Monitoring**: Implement logging and monitoring solutions

### Generate Secure Secret Key
```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

## Advanced Configuration

### Custom Dockerfile
Create `Dockerfile.local` for development:
```dockerfile
FROM python:3.11-slim-bookworm
# Your custom configuration
```

### Multi-stage Build
For smaller production images, modify Dockerfile to use multi-stage builds.

### Health Checks
The container includes health checks. Monitor with:
```bash
docker inspect firefly-app | grep -A5 Health
```

## Integration with CI/CD

### GitHub Actions Example
```yaml
name: Build and Deploy Firefly
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build and test
        run: docker-compose up --build -d && docker-compose down
```

## Support

For issues specific to the Docker setup:
1. Check this documentation
2. Review Docker and docker-compose logs
3. Open an issue on the GitHub repository

For general Firefly support, refer to the main README.md file.