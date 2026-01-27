# Training Commodity Carl (CC)

## Overview

This folder contains recordings, transcripts, and process documentation that will be used to train CC to understand agricultural commodity analysis workflows.

## Folder Structure

```
training/
├── recordings/          # Screen recordings (.mp4, .webm)
├── transcripts/         # AI-generated transcripts (.txt, .json)
├── process_docs/        # Structured process documentation (.md)
└── CC_TRAINING_GUIDE.md # This file
```

## Quick Start: Recording a Process

### Option 1: ScreenApp.io (Recommended - Free tier available)

1. Go to [screenapp.io](https://screenapp.io)
2. Click "Record" - no signup needed for basic recording
3. Select "Screen + Audio"
4. Record yourself updating a balance sheet
5. After recording, it auto-transcribes
6. Download both the video and transcript
7. Save to `training/recordings/` and `training/transcripts/`

### Option 2: Windows Built-in (Xbox Game Bar)

1. Press `Win + G` to open Game Bar
2. Click the microphone icon to enable audio
3. Press `Win + Alt + R` to start recording
4. Narrate what you're doing as you work
5. Press `Win + Alt + R` again to stop
6. Videos save to `Videos/Captures/`
7. Use Whisper to transcribe (see below)

### Option 3: OBS Studio (Most control)

1. Download OBS from [obsproject.com](https://obsproject.com)
2. Add "Display Capture" source
3. Add "Audio Input Capture" for microphone
4. Start recording, narrate your process
5. Use Whisper to transcribe

## Transcribing with Whisper

If you use a recorder without auto-transcription:

```powershell
# Install whisper (one time)
pip install openai-whisper

# Transcribe a recording
whisper "recordings/my_recording.mp4" --output_dir transcripts/ --output_format txt
```

## What to Record

### Priority 1: Balance Sheet Updates
- [ ] Updating World Soybean balance sheet after WASDE
- [ ] Updating US Corn balance sheet
- [ ] Updating Brazil soybean estimates
- [ ] Updating ethanol production numbers

### Priority 2: Data Analysis
- [ ] Calculating stocks-to-use ratios
- [ ] Comparing RLC estimates to USDA
- [ ] Analyzing crush margins
- [ ] Reviewing export pace vs forecast

### Priority 3: Market Commentary
- [ ] Morning market review process
- [ ] Weekly summary preparation
- [ ] Client call preparation

## Recording Best Practices

1. **Narrate everything** - Explain your thinking out loud
   - "I'm updating Brazil production because CONAB released new numbers..."
   - "I'm checking this against last month's estimate..."

2. **Explain the WHY** - Not just what you're doing
   - "We use Oct-Sep marketing year for soybeans because..."
   - "I'm skeptical of this number because historically..."

3. **Point out relationships**
   - "When crush goes up, meal production also increases..."
   - "Higher ending stocks usually means lower prices..."

4. **Highlight sources**
   - "This number comes from the WASDE report..."
   - "CONAB releases this data monthly..."

5. **Mention edge cases**
   - "Sometimes this cell is blank, which means..."
   - "Be careful here because the units change..."

## File Naming Convention

```
recordings/
  2024-12-23_wasde_soybean_update.mp4
  2024-12-23_brazil_corn_estimate.mp4

transcripts/
  2024-12-23_wasde_soybean_update.txt
  2024-12-23_brazil_corn_estimate.txt

process_docs/
  wasde_update_process.md
  brazil_estimate_workflow.md
```

## Converting Transcripts to Training Data

After you have transcripts, CC can help convert them into:

1. **Step-by-step procedures**
2. **Decision trees** (if X, do Y)
3. **Data relationship maps**
4. **Common patterns and exceptions**

Run this to process transcripts:
```powershell
python training/process_transcript.py transcripts/my_recording.txt
```

## Progress Tracker

| Process | Recorded | Transcribed | Documented |
|---------|----------|-------------|------------|
| WASDE soybean update | [ ] | [ ] | [ ] |
| WASDE corn update | [ ] | [ ] | [ ] |
| Brazil production update | [ ] | [ ] | [ ] |
| Argentina production update | [ ] | [ ] | [ ] |
| Ethanol balance sheet | [ ] | [ ] | [ ] |
| Crush margin calculation | [ ] | [ ] | [ ] |
| Export pace analysis | [ ] | [ ] | [ ] |

---

*Last updated: 2024-12-23*
