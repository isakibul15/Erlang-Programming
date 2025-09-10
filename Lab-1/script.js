// script.js
console.log("Rudy Server JavaScript loaded!");
document.addEventListener('DOMContentLoaded', function() {
    const button = document.createElement('button');
    button.textContent = 'Click me!';
    button.onclick = function() {
        alert('Hello from Rudy Server!');
    };
    document.body.appendChild(button);
});