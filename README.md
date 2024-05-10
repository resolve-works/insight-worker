
# Insight Worker

Worker service for [Insight](https://github.com/followthemoney/insight/).
Handles various tasks like indexing, embedding and answering prompts.

### Development

Copy over `.env.example` to `.env`:
```
cp .env.example .env
```

Now set your `OPENAI_API_KEY` in the newly created `.env` file.


You'll also need some system dependencies to run the worker locally. For Arch
Linux this would be:
```
sudo pacman -S tesseract tesseract-data-nld tesseract-data-eng pngquant ghostscript
```

```
yay -S jbig2enc
```
