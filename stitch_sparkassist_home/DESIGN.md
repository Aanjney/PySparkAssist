# Design System Specification: The Ethereal Assistant

## 1. Overview & Creative North Star: "The Digital Curator"
This design system moves away from the cluttered, "utility-first" look of traditional AI tools toward a high-end, editorial experience. Our Creative North Star is **The Digital Curator**. 

The goal is to make SparkAssist feel like a calm, high-end gallery space rather than a terminal. We achieve this through **Intentional Asymmetry** (breaking the perfectly centered grid with floating action elements), **Tonal Depth** (using color shifts instead of lines), and **Generous Negative Space**. By utilizing the `manrope` display face alongside a strict "No-Line" policy, we create a UI that feels breathed into existence, not built with blocks.

---

## 2. Colors & Surface Philosophy
The palette is rooted in a "Soft High-Key" aesthetic—predominantly whites and cool greys, punctuated by a deep, intelligent violet (`primary`).

### The "No-Line" Rule
**Explicit Instruction:** Do not use 1px solid borders to define sections. Traditional dividers are forbidden. 
*   **How to separate:** Use background shifts. A `surface-container-low` input area should sit directly on a `surface` background. The transition of tone is the boundary.
*   **Nesting:** Depth is created by "stepping" through the container tiers.
    *   *App Background:* `surface` (#f8f9fa)
    *   *Main Chat Area:* `surface-container-lowest` (#ffffff)
    *   *Sidebars/Overlays:* `surface-container` (#eaeff1)

### The "Glass & Gradient" Rule
To elevate the "Spark" in SparkAssist, use **Glassmorphism** for floating elements (like a sticky header or a hovering prompt bar). 
*   **Formula:** `surface_container_lowest` at 70% opacity + `backdrop-blur: 20px`.
*   **Signature Textures:** Use a subtle linear gradient from `primary` (#4d44e3) to `primary_dim` (#4034d7) for primary CTAs to give them a "jewel-like" depth against the matte background.

---

## 3. Typography: Editorial Authority
We pair the geometric elegance of **Manrope** for headers with the functional clarity of **Inter** for the AI’s responses.

*   **Display & Headlines (Manrope):** Use `display-md` for empty-state greetings. The tight tracking and generous scale convey a "premium magazine" feel.
*   **Body (Inter):** AI responses must use `body-lg` (1rem). The slightly larger scale ensures readability and reduces eye strain in long conversations.
*   **Hierarchy Tip:** Use `on_surface_variant` (#586064) for timestamps and "AI is typing" indicators to keep them present but non-distracting.

---

## 4. Elevation & Depth: Tonal Layering
Traditional drop shadows are too "heavy" for this system. We use **Ambient Light** principles.

*   **The Layering Principle:** Place a `surface-container-lowest` card (the chat bubble) on a `surface-container-low` background to create a soft, natural lift without a single pixel of shadow.
*   **Ambient Shadows:** When a component must float (e.g., a settings modal), use:
    *   *Shadow Color:* `on_surface` (#2b3437) at 4% opacity.
    *   *Blur:* 40px to 60px.
*   **The "Ghost Border" Fallback:** If accessibility requires a container edge, use `outline-variant` (#abb3b7) at **15% opacity**. It should be felt, not seen.

---

## 5. Components: The Assistant’s Kit

### Chat Bubbles (Cards & Lists)
*   **User Bubbles:** `primary_container` (#e2dfff) with `on_primary_container` text. Use `md` (0.75rem) roundedness.
*   **AI Bubbles:** `surface_container_lowest` (#ffffff) with no border. 
*   **Spacing:** Use `spacing-6` (2rem) between message groups to emphasize the "editorial" layout. **No dividers.**

### Primary Input Field
*   **Styling:** A wide, pill-shaped (`rounded-full`) container using `surface_container_high`. 
*   **Interaction:** On focus, transition background to `surface_container_lowest` and apply the Ambient Shadow. Do not use a high-contrast focus ring; use a subtle `primary` glow (20% opacity).

### Buttons & Chips
*   **Primary Button:** Gradient-filled (`primary` to `primary_dim`). Use `sm` (0.25rem) roundedness for a sharper, modern professional look.
*   **Action Chips:** Use `secondary_container` with `on_secondary_container` text. These should be `rounded-full` to distinguish them from structural elements.

### Specialized AI Components
*   **Streaming Indicator:** Instead of a blinking cursor, use a soft pulse effect on a `primary` colored dot.
*   **Code Blocks:** Use `inverse_surface` (#0c0f10) with `surface_variant` text. This provides the only high-contrast "moment" in the UI, drawing focus to the technical output.

---

## 6. Do’s and Don’ts

### Do
*   **Do** use `spacing-16` (5.5rem) for top and bottom margins to frame the conversation.
*   **Do** use `manrope` in `headline-lg` for the "SparkAssist" logo lockup, keeping it lowercase for a modern, approachable feel.
*   **Do** use asymmetrical placement for the "New Chat" button—perhaps floating in the bottom right using Glassmorphism.

### Don’t
*   **Don't** use pure black (#000). Use `on_surface` (#2b3437) for all dark text to maintain the soft palette.
*   **Don't** use 1px dividers between chat messages. Use `spacing-4` (1.4rem) of vertical whitespace instead.
*   **Don't** use standard "system blue" for links. Always use the `primary` violet token.