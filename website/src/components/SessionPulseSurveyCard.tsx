import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { X } from 'lucide-react'
import { useTranslation } from 'react-i18next'

// Kiro Crew is self-hosted, open-source software — every install runs on its
// own arbitrary origin, which Aperture's browser-CORS allowlist model (a
// finite, known set of domains) cannot accommodate. These calls go to the
// Kiro Crew backend's own same-origin routes instead
// (src/kiro_crew/dashboard/handlers/feedback.py), which forward to Aperture
// server-to-server, where CORS does not apply.
const FEEDBACK_SUBMIT_URL = '/api/feedback/submit'
const FEEDBACK_ELIGIBLE_URL = '/api/feedback/eligible'

// Response values Aperture's registered template expects verbatim for this
// radio question — these are wire values sent as `responseValue`, not display
// copy, so they stay fixed English regardless of locale (see RATING_LABEL_KEYS
// below for the translated label shown to the user).
const RATING_OPTIONS = ['Very Poor', 'Poor', 'Fair', 'Good', 'Excellent'] // brand-ok: not applicable, literal Aperture wire values

const RATING_LABEL_KEYS: Record<string, string> = {
  'Very Poor': 'components.sessionPulseSurveyCard.rating_very_poor',
  Poor: 'components.sessionPulseSurveyCard.rating_poor',
  Fair: 'components.sessionPulseSurveyCard.rating_fair',
  Good: 'components.sessionPulseSurveyCard.rating_good',
  Excellent: 'components.sessionPulseSurveyCard.rating_excellent',
}

// Verbatim registered Aperture template text (GET rendering.../form/template
// for category=KiroCrew, name=SessionFeedback, version=1.0.1) — ingestion  // brand-ok: registered category id
// 400s on any text mismatch against the form template, so this cannot be
// localized. Shared between the visible label and its aria-label so a screen
// reader hears the same question a sighted user reads.
const RATING_QUESTION_TEXT = 'How would you rate your experience with KiroCrew today?' // brand-ok: verbatim registered template text, ingestion 400s on mismatch

// How long the "Thanks for your feedback" confirmation stays visible after a
// successful submit, before the card fades away on its own.
const CONFIRMATION_DISPLAY_MS = 3000

// Aperture tracks its own per-user eligibility server-side, but its form-level
// cooldown isn't something we control from the client, and Mia wants a firm
// 30-day cooldown regardless of Aperture's configured default. We ask
// Aperture first (so it still gets accurate per-user, cross-device dedup
// data), then additionally require our own 30-day gate before showing —
// whichever check is stricter wins.
const COOLDOWN_KEY = 'kirocrew_survey_last_shown'
const COOLDOWN_DAYS = 30

function localCooldownElapsed(): boolean {
  const lastShown = localStorage.getItem(COOLDOWN_KEY)
  if (!lastShown) return true
  const elapsed = Date.now() - new Date(lastShown).getTime()
  return elapsed > COOLDOWN_DAYS * 24 * 60 * 60 * 1000
}

function markLocalCooldown(): void {
  localStorage.setItem(COOLDOWN_KEY, new Date().toISOString())
}

/** Ask (via our own backend) whether Aperture considers this user due for the
 * survey. A failure of any kind (network, non-2xx) fails closed — don't show
 * the survey rather than guessing eligibility. */
async function checkSurveyEligible(userId: string): Promise<boolean> {
  try {
    const res = await fetch(
      `${FEEDBACK_ELIGIBLE_URL}?userId=${encodeURIComponent(userId)}`
    )
    if (!res.ok) return false
    const body = await res.json().catch(() => null)
    return body?.eligible === true
  } catch {
    return false
  }
}

interface SessionPulseSurveyCardProps {
  sessionId: string
  userId: string
  kiroCrewVersion: string
  turnCount: number
  /** Notifies the parent when shown/hidden, so it can re-anchor scroll
   * position the same way it does for other in-flow bands (see the
   * activeTip re-anchor effect in ChatPage) — this component sits outside
   * the virtualizer's measured rows, so a mount/unmount here changes the
   * scroll viewport's real content height without the virtualizer knowing. */
  onVisibleChange?: (visible: boolean) => void
}

export default function SessionPulseSurveyCard({
  sessionId,
  userId,
  kiroCrewVersion,
  turnCount,
  onVisibleChange,
}: SessionPulseSurveyCardProps) {
  const { t } = useTranslation()
  const [visible, setVisible] = useState(false)
  const [selectedRating, setSelectedRating] = useState<string | null>(null)
  const [feedback, setFeedback] = useState('')
  const [email, setEmail] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [submitted, setSubmitted] = useState(false)
  const [submitError, setSubmitError] = useState(false)
  // Baseline captured once per session mount (this component remounts on
  // session switch via `key={activeSlot}`). turnCount includes every
  // assistant turn already loaded from history, so without a baseline,
  // reopening any session with >=3 prior turns would pop the survey on every
  // visit — merely re-reading old messages, not a fresh interaction. Only
  // turns completed live past this baseline count toward eligibility.
  const [baselineTurnCount] = useState(turnCount)
  const liveTurnCount = turnCount - baselineTurnCount
  // Without this guard, crossing the turn-3 threshold re-fires the effect on
  // every subsequent turn (4th, 5th, ...) as long as the card stays hidden —
  // each one re-hitting /api/feedback/eligible for an answer that cannot
  // change mid-session. Check at most once per mount.
  const [eligibilityChecked, setEligibilityChecked] = useState(false)

  useEffect(() => {
    if (liveTurnCount < 3 || visible || eligibilityChecked || !localCooldownElapsed()) return
    let cancelled = false
    setEligibilityChecked(true)
    checkSurveyEligible(userId).then((eligible) => {
      if (!cancelled && eligible) {
        setVisible(true)
        markLocalCooldown()
      }
    })
    return () => {
      cancelled = true
    }
  }, [liveTurnCount, userId, visible, eligibilityChecked])

  useEffect(() => {
    onVisibleChange?.(visible)
  }, [visible, onVisibleChange])

  const dismiss = () => setVisible(false)

  const submit = async () => {
    if (!selectedRating) return
    setSubmitting(true)
    setSubmitError(false)

    try {
      const res = await fetch(FEEDBACK_SUBMIT_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          rating: selectedRating,
          feedback: feedback.trim(),
          email: email.trim(),
          sessionId,
          kiroCrewVersion,
          userId,
        }),
      })
      if (!res.ok) {
        // Aperture rejected the submission or is unavailable — keep the form
        // visible so the user can retry, rather than showing a false
        // confirmation and burning the 30-day cooldown on lost feedback.
        setSubmitting(false)
        setSubmitError(true)
        return
      }
    } catch {
      setSubmitting(false)
      setSubmitError(true)
      return
    }

    setSubmitting(false)
    setSubmitted(true)
    setTimeout(() => setVisible(false), CONFIRMATION_DISPLAY_MS)
  }

  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.95 }}
          transition={{ type: 'spring', bounce: 0, duration: 0.28 }}
          className="border border-accent/30 rounded-xl bg-card shadow-sm mt-3"
        >
          <div className="px-4 py-4 relative">
            {/* Dismiss */}
            <button
              onClick={dismiss}
              aria-label={t('components.sessionPulseSurveyCard.dismiss')}
              className="absolute top-3 right-3 bg-transparent border-none text-muted hover:text-text cursor-pointer"
            >
              <X size={16} />
            </button>

            {submitted ? (
              <p className="text-[14px] font-medium text-text pr-6">
                {t('components.sessionPulseSurveyCard.thanks')}
              </p>
            ) : (
              <>
                {/* Rating */}
                <div className="mb-5">
                  <p className="text-[14px] font-medium text-text mb-3">
                    {RATING_QUESTION_TEXT}
                  </p>
                  <div className="flex gap-2 flex-wrap" role="radiogroup" aria-label={RATING_QUESTION_TEXT}>
                    {RATING_OPTIONS.map((option) => (
                      <button
                        key={option}
                        onClick={() => setSelectedRating(option)}
                        aria-pressed={selectedRating === option}
                        className={`text-left px-3 py-2 rounded-lg text-[13px] cursor-pointer transition-all border font-medium ${
                          selectedRating === option
                            ? 'border-accent text-text bg-accent-subtle/60'
                            : 'border-border text-muted hover:text-text hover:border-accent/40 bg-bg font-normal'
                        }`}
                      >
                        {t(RATING_LABEL_KEYS[option])}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Open feedback */}
                <div className="mb-4">
                  <p className="text-[13px] text-text mb-2">
                    {t('components.sessionPulseSurveyCard.feedback_question')}
                  </p>
                  <textarea
                    value={feedback}
                    onChange={(e) => setFeedback(e.target.value)}
                    placeholder={t('components.sessionPulseSurveyCard.optional')}
                    className="w-full px-3 py-2 rounded-lg border border-border bg-bg text-text text-[13px] placeholder:text-muted focus:border-accent focus:outline-none resize-vertical min-h-[60px]"
                  />
                </div>

                {/* Email */}
                <div className="mb-4">
                  <p className="text-[13px] text-text mb-1">
                    {t('components.sessionPulseSurveyCard.email_prompt')}
                  </p>
                  <p className="text-[11px] text-muted mb-2">
                    {t('components.sessionPulseSurveyCard.email_disclosure')}
                  </p>
                  <input
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder={t('components.sessionPulseSurveyCard.email_placeholder')}
                    className="w-full px-3 py-2 rounded-lg border border-border bg-bg text-text text-[13px] placeholder:text-muted focus:border-accent focus:outline-none"
                  />
                </div>

                {/* Submit */}
                <div className="flex items-center justify-end gap-3">
                  {submitError && (
                    <p className="text-[12px] text-danger">
                      {t('components.sessionPulseSurveyCard.submit_error')}
                    </p>
                  )}
                  {!submitError && !selectedRating && (
                    <p className="text-[12px] text-muted">
                      {t('components.sessionPulseSurveyCard.select_rating_hint')}
                    </p>
                  )}
                  <button
                    onClick={submit}
                    disabled={!selectedRating || submitting}
                    className="px-3.5 py-1.5 rounded-md text-[13px] font-medium bg-accent text-accent-fg hover:bg-accent-hover border-none disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer transition-all"
                  >
                    {submitting
                      ? t('components.sessionPulseSurveyCard.submitting')
                      : t('components.sessionPulseSurveyCard.submit')}
                  </button>
                </div>
              </>
            )}
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  )
}
