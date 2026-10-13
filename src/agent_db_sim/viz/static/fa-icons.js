/* Minimal UMD-style shim providing just the Font Awesome solid icons that
 * d3-wave consumes, so the waveform view runs fully offline (no CDN).
 * Icon data copied from @fortawesome/free-solid-svg-icons 6.7.2 (CC BY 4.0).
 * d3-wave's UMD bundle looks up the global key "free-solid-svg-icons".
 */
(function (global) {
    'use strict';
    var fas = 'fas';
    var icons = {
        faChevronDown: { prefix: fas, iconName: 'chevron-down', icon: [512, 512, [], 'f078', 'M233.4 406.6c12.5 12.5 32.8 12.5 45.3 0l192-192c12.5-12.5 32.8-12.5 45.3 0s12.5 32.8 0 45.3L256 338.7 86.6 169.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l192 192z'] },
        faChevronRight: { prefix: fas, iconName: 'chevron-right', icon: [512, 512, [], 'f054', 'M310.6 233.4c12.5 12.5 12.5 32.8 0 45.3l-192 192c-12.5 12.5-32.8 12.5-45.3 0s-12.5-32.8 0-45.3L242.7 256 73.4 86.6c-12.5-12.5-12.5-32.8 0-45.3s32.8-12.5 45.3 0l192 192z'] },
        faDownload: { prefix: fas, iconName: 'download', icon: [512, 512, [], 'f019', 'M288 32c0-17.7-14.3-32-32-32s-32 14.3-32 32l0 242.7-73.4-73.4c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3l128 128c12.5 12.5 32.8 12.5 45.3 0l128-128c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0L288 274.7 288 32zM64 352c-35.3 0-64 28.7-64 64l0 32c0 35.3 28.7 64 64 64l384 0c35.3 0 64-28.7 64-64l0-32c0-35.3-28.7-64-64-64l-101.5 0-45.3 45.3c-25 25-65.5 25-90.5 0L165.5 352 64 352zm368 56a24 24 0 1 1 0 48 24 24 0 1 1 0-48z'] },
        faQuestion: { prefix: fas, iconName: 'question', icon: [512, 512, [], '3f', 'M80 160c0-35.3 28.7-64 64-64l32 0c35.3 0 64 28.7 64 64l0 3.6c0 21.8-11.1 42.1-29.4 53.8l-42.2 27.1c-25.2 16.2-40.4 44.1-40.4 74l0 1.4c0 17.7 14.3 32 32 32s32-14.3 32-32l0-1.4c0-8.2 4.2-15.8 11-20.2l42.2-27.1c36.6-23.6 58.8-64.1 58.8-107.7l0-3.6c0-70.7-57.3-128-128-128l-32 0C73.3 32 16 89.3 16 160c0 17.7 14.3 32 32 32s32-14.3 32-32zm80 320a40 40 0 1 0 0-80 40 40 0 1 0 0 80z'] },
    };
    global['free-solid-svg-icons'] = { prefix: fas, __esModule: true, ...icons };
})(window);
