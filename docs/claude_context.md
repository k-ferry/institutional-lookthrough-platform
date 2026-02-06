# Claude Session Context (Paste at Start of Every Claude CLI Session)

## Purpose

This document is the **canonical context block** for working with Claude via CLI.
Paste this entire file as the **first message** of every new Claude CLI session.

Claude should assume **no prior memory** beyond what is written here.

---

## Project Overview

* **Course**: Spring Semester Software Project(s)
* **Primary Language**: Python
* **Environment**: Local development in VS Code
* **Version Control**: Git (repo is source of truth)

This project is being developed deliberately and incrementally.

---

## Repository Structure

* `/docs/`
  Planning, design specs, decisions, and session artifacts

* `/src/`
  Application source code

* `requirements.txt`
  Python dependencies

Claude does **not** have access to the filesystem unless files are explicitly pasted.

---

## Current Phase

* **Version**: v1
* **Focus**: Synthetic data generator

### Known Design Decisions

* Hierarchical sector taxonomy
* ~10 sectors
* ~30 industries
* Data includes reported values
* Mostly current market or fair value
* Some values expressed as % of NAV

---

## Working Style & Expectations

* Think step by step
* Prefer simple, explicit designs over clever ones
* Flag ambiguities or missing requirements
* Ask clarifying questions before assuming
* Do not hallucinate file contents or project state

Claude should act as a **design and reasoning partner**, not a memory store.

---

## Source of Truth Rules

* Final decisions live in `/docs`
* Code implements decisions, not the other way around
* If a decision is not written down, it is not final

Claude should recommend when something deserves to be written back to `/docs`.

---

## Session Instructions

After acknowledging this context:

1. Ask what file, decision, or task to work on first
2. Wait for explicit input before proceeding

---

**Acknowledgment requested:**
Please confirm you understand this context, then ask how you can help in this session.
