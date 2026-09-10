/* assessment_monitor.js — the MCQ assessment's activity monitor.
   The interview page's proven detector bundle (tab/focus with durations,
   copy/paste flags, reload/reopen, inactivity), DOM-only — no camera, no
   WebRTC. Data shapes match the interview's proctoring exactly:
   flags[{t,type,reason,dur?}], counts{type:n}, durations{key:s} — so the
   server validator and the recruiter renderer stay one family.
   HONESTY: everything here is a REVIEW SIGNAL for a human. Nothing blocks,
   ejects, or scores. The page discloses all of it before question 1. */
(function(){
  'use strict';
  var M = {
    startMs: 0, flags: [], counts: {}, durations: {},
    awaySince: 0, awayReason: '', lastActivity: 0,
    pasteMode: 'restrict', currentQ: -1, inactTimer: null, armed: false,
  };
  function nowT(){ return Math.round((Date.now() - M.startMs) / 1000); }
  function flag(type, reason, dur){
    M.counts[type] = (M.counts[type] || 0) + 1;
    var f = { t: nowT(), type: type, reason: reason };
    if (dur != null) f.dur = Math.round(dur);
    M.flags.push(f);
    if (M.flags.length > 120) M.flags.shift();   // bounded; server caps again
  }
  function qLabel(){ return M.currentQ >= 0 ? ' (question ' + (M.currentQ + 1) + ')' : ''; }

  function onAway(reason){
    if (!M.armed || M.awaySince) return;
    M.awaySince = Date.now(); M.awayReason = reason;
  }
  function onBack(){
    if (!M.awaySince) return;
    var dur = (Date.now() - M.awaySince) / 1000; M.awaySince = 0;
    var k = M.awayReason === 'window lost focus' ? 'focus_loss' : 'tab_away';
    M.durations[k] = (M.durations[k] || 0) + dur;
    flag('tab_switch', 'left the assessment (' + M.awayReason + ')' + qLabel(), dur);
  }
  function bump(){ M.lastActivity = Date.now(); }
  function inactTick(){
    if (!M.armed || M.awaySince) return;          // away time is counted separately
    var idle = (Date.now() - M.lastActivity) / 1000;
    if (idle >= 90){
      flag('inactivity', 'no input for ' + Math.round(idle) + 's' + qLabel(), idle);
      M.lastActivity = Date.now();                // one flag per stretch
    }
  }

  window.AssessMonitor = {
    /* opts: { pasteMode: 'restrict'|'monitor', storageKey: string } */
    start: function(opts){
      opts = opts || {};
      M.pasteMode = opts.pasteMode === 'monitor' ? 'monitor' : 'restrict';
      M.startMs = Date.now(); M.lastActivity = Date.now(); M.armed = true;
      // Reload/reopen: a sessionStorage counter survives refreshes — each
      // restart of this monitor past the first is a recorded reload.
      try {
        var k = (opts.storageKey || 'am') + '-loads';
        var n = parseInt(sessionStorage.getItem(k) || '0') + 1;
        sessionStorage.setItem(k, String(n));
        if (n > 1) flag('refresh', 'page reloaded/reopened during the assessment (load #' + n + ')');
      } catch(_){}
      document.addEventListener('visibilitychange', function(){
        if (document.hidden) onAway('tab hidden'); else onBack();
      });
      window.addEventListener('blur', function(){ onAway('window lost focus'); });
      window.addEventListener('focus', function(){ onBack(); });
      ['mousemove','keydown','pointerdown','scroll','touchstart'].forEach(function(ev){
        window.addEventListener(ev, bump, {passive: true});
      });
      document.addEventListener('copy', function(){ flag('copy', 'copied from the assessment' + qLabel()); });
      document.addEventListener('cut',  function(){ flag('cut',  'cut from the assessment' + qLabel()); });
      document.addEventListener('paste', function(e){
        flag('paste', (M.pasteMode === 'restrict' ? 'paste attempted (blocked)'
                                                  : 'paste (allowed, recorded)') + qLabel());
        if (M.pasteMode === 'restrict') e.preventDefault();
      });
      M.inactTimer = setInterval(inactTick, 10000);
    },
    noteQuestion: function(i){ M.currentQ = i; bump(); },
    summary: function(){
      onBack();   // close any open away-stretch into the totals
      return {
        enabled: true, flags_schema: 1, mode: M.pasteMode,
        flags: M.flags, counts: M.counts,
        durations: (function(d){ var o = {}; for (var k in d) o[k] = Math.round(d[k]); return o; })(M.durations),
        total_seconds: nowT(),
      };
    },
  };
})();
