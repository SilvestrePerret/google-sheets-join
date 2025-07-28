# Pair Programming Workflow

## Overview
This document outlines our pair programming workflow for building Google Sheets Join. It ensures consistent process, proper testing, and knowledge capture as we work through the project plan.

## Story Workflow Process

### 1. Story Selection & Planning
**Claude's Actions:**
- Pick up the next ⏳ **Pending** story from the `project_plan` markdown file
- Review the story's acceptance criteria and testing requirements
- Create a brief implementation plan (2-3 bullet points)
- **Confirm plan with human before proceeding**

### 2. Implementation Preparation
**Claude's Actions:**
- After brief plan is approved by human, create a comprehensive implementation plan
- Build a detailed todo list for the story implementation using TodoWrite tool
- Review `CLAUDE.md` for project-specific commands and architecture notes
- Update todo list to mark story as "in_progress"
- Read any relevant existing files to understand current state

### 3. Implementation
**Claude's Actions:**
- Implement the story following acceptance criteria
- Write clean, well-structured code following established patterns
- Use proper error handling where needed

### 4. Human Verification Request
**Claude's Actions:**
- Summarize what was implemented
- List what should be visible/testable in the app

### 5. Human Testing & Feedback
**Human's Actions:**
- Test the changes in the 
- Verify acceptance criteria are met
- Provide feedback: "Perfect! That worked!" or describe issues

**If Issues Found:**
- Human describes what went wrong or what's not working
- Claude investigates and fixes the issues
- Repeat until verification succeeds

### 7. Story Completion
**Claude's Actions (after human confirms success):**
- Mark story as ✅ **Completed** in the `project_plan` markdown file
- Update todo list to completed
- Update progress tracking in the `project_plan` markdown file
- Update technical_considerations.md with lessons learned

## Documentation Updates

### During Each Story
**Claude should update these files as needed:**

#### CLAUDE.md Updates
- Add new commands discovered during implementation
- Update architecture notes if patterns change
- Document any new dependencies or tools
- Note any gotchas or important considerations

#### technical_considerations.md Updates
- Document technical decisions and rationale
- Record issues encountered and solutions
- Note what works well and what to avoid
- Capture performance considerations
- Document testing strategies that work

## Quality Checks

### Before Marking Story Complete
- [ ] All acceptance criteria met
- [ ] All specified tests pass
- [ ] No new console errors or warnings  
- [ ] Code follows project conventions
- [ ] Relevant documentation updated
- [ ] Human verification completed

### Code Quality Standards
- Use Google Script (javascript) properly
- Handle errors gracefully
- Write clear, self-documenting code
- Keep functions small and focused

## Communication Patterns

### Starting a Story
Claude: "Ready for Story X.Y: [Title]. My plan: [brief plan]. Does this approach sound good?"

### During Implementation
Claude: [Work silently through implementation, testing, and verification]

### Requesting Verification
Claude: "Story X.Y implementation complete! Changes: [summary]. Please verify: [specific things to check]. Ready for your verification!"

### After Human Confirmation
Claude: "Great! Marking Story X.Y as completed. Moving to next story..."

## Emergency Procedures

### If Story Becomes Too Complex
- Break it down into smaller sub-tasks
- Document the breakdown in `technical_considerations.md`
- Get human approval for the new approach

### If Architecture Changes Needed
- Document the change rationale in `technical_considerations.md`
- Update `google-sheets-join-architecture.md`
- Get human approval before major changes

### If Stuck on Technical Issue
- Document the issue in `technical_considerations.md`
- Propose 2-3 alternative approaches
- Ask human for guidance

## Success Metrics
- Stories completed per session
- Build success rate (should be 100%)
- Quality of human verification feedback
- Documentation completeness
- Knowledge capture effectiveness

---

This workflow ensures we maintain quality, capture knowledge, and make steady progress toward our MVP goal.
