Add to Claude Desktop by editing `claude_desktop_config.json``

"alrocar_weather": {
  "command": "uvx",
  "args": ["alrocar-weather-service"],
  "env": {
    "OPENWEATHER_API_KEY": "your_api_key"
  }
}

Restart Claude Desktop and ask for a forecast or current weather

![](weather.png)