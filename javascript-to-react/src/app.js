const container = document.querySelector(".container");
const text = document.getElementById("text");

let totalTime = 7500; // 7.5s
let breathTime = (totalTime / 5) * 2; // 3s
let holdTime = totalTime / 5; //1.5s

brathAnimation();

function brathAnimation() {
  text.innerHTML = "吸って！";
  container.className = "container grow";
  setTimeout(() => {
    text.innerHTML = "止めて！";
    setTimeout(() => {
      text.innerHTML = "吐いて！";
      container.className = "container shrink";
    }, holdTime);
  }, breathTime);
}

setInterval(brathAnimation, totalTime);
